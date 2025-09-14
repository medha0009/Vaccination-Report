import os
import sys
import traceback
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# ======== CONFIG ========
BASE_PATH = r"C:\Users\medha\OneDrive\Desktop\Vaccination"  # folder with Excel files
FILES = {
    "coverage":      "cleaned_coverage_data.xlsx",
    "incidence":     "cleaned_incidence_rate_data.xlsx",
    "reported":      "cleaned_reported_cases_data.xlsx",
    "introduction":  "cleaned_vaccine_introduction_data.xlsx",
    "schedule":      "cleaned_vaccine_schedule_data.xlsx",
}
DB = dict(
    host="localhost",
    user="root",
    password="123456",      # <<< change me
    database="vaccination",      # <<< change me
)

# ======== UTILITIES ========
def safe_read_excel(path):
    fp = os.path.join(BASE_PATH, path)
    if not os.path.exists(fp):
        print(f"⚠️  Missing file: {path} (skipping)")
        return None
    try:
        return pd.read_excel(fp)
    except PermissionError:
        print(f"⚠️  Permission denied reading {path}. "
              f"Close the file in Excel/OneDrive and re-run. Skipping for now.")
        return None
    except Exception as e:
        print(f"⚠️  Could not read {path}: {e}. Skipping.")
        return None

def get_table_columns(cur, table):
    cur.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
    """, (table,))
    return {r[0].lower() for r in cur.fetchall()}

def choose(colset, *candidates):
    """Pick the first existing column from candidates (case-insensitive)."""
    for c in candidates:
        if c and c.lower() in colset:
            return c.lower()
    return None

def fetch_id_map(cur, table, key_col, id_col="id"):
    """Return a dict key -> id for a master table."""
    cur.execute(f"SELECT {id_col}, {key_col} FROM {table}")
    m = {}
    for _id, key in cur.fetchall():
        if key is None:
            continue
        m[str(key).strip().upper()] = _id
    return m

def normalize_str(x):
    if pd.isna(x):
        return None
    return str(x).strip()

def to_int(x):
    try:
        if pd.isna(x) or x == "":
            return None
        return int(float(x))
    except Exception:
        return None

def to_dec(x):
    try:
        if pd.isna(x) or x == "":
            return None
        return float(x)
    except Exception:
        return None

# ======== MAIN ========
def main():
    # connect db
    try:
        cnx = mysql.connector.connect(**DB)
    except mysql.connector.Error as err:
        print("❌ DB connection failed:", err)
        sys.exit(1)

    cur = cnx.cursor()

    # Load files
    coverage_df     = safe_read_excel(FILES["coverage"])
    incidence_df    = safe_read_excel(FILES["incidence"])
    reported_df     = safe_read_excel(FILES["reported"])
    introduction_df = safe_read_excel(FILES["introduction"])
    schedule_df     = safe_read_excel(FILES["schedule"])

    # Trim/clean if loaded
    for df in [coverage_df, incidence_df, reported_df, introduction_df, schedule_df]:
        if df is None:
            continue
        df.columns = df.columns.str.strip()
        for c in df.select_dtypes(include=["object"]).columns:
            df[c] = df[c].astype(str).str.strip()
        df.drop_duplicates(inplace=True)

    # ---- countries master ----
    if introduction_df is not None:
        countries_cols = get_table_columns(cur, "countries")
        iso_col  = choose(countries_cols, "iso_code", "iso3", "code", "iso_3_code")
        name_col = choose(countries_cols, "country_name", "name")
        reg_col  = choose(countries_cols, "who_region", "region")

        if not (iso_col and name_col):
            print("❌ 'countries' table must have at least iso_code & country_name (or synonyms).")
            sys.exit(1)

        # Source columns expected in introduction_df
        src_iso  = "ISO_3_CODE" if "ISO_3_CODE" in introduction_df.columns else None
        src_name = "COUNTRYNAME" if "COUNTRYNAME" in introduction_df.columns else None
        src_reg  = "WHO_REGION" if "WHO_REGION" in introduction_df.columns else None

        if not (src_iso and src_name):
            print("⚠️  Introduction file lacks ISO_3_CODE/COUNTRYNAME; skipping countries insert.")
        else:
            ctry = introduction_df[[src_iso, src_name] + ([src_reg] if src_reg else [])].drop_duplicates()
            print(f"✅ Upserting {len(ctry)} countries...")
            placeholders = ", ".join(["%s"] * (2 + (1 if src_reg else 0)))
            cols = [iso_col, name_col] + ([reg_col] if src_reg and reg_col else [])
            sql = f"INSERT IGNORE INTO countries ({', '.join(cols)}) VALUES ({placeholders})"
            for _, r in ctry.iterrows():
                vals = [normalize_str(r[src_iso]), normalize_str(r[src_name])]
                if src_reg and reg_col:
                    vals.append(normalize_str(r[src_reg]))
                cur.execute(sql, tuple(vals))
            cnx.commit()

    # ---- diseases master ----
    if (incidence_df is not None) or (reported_df is not None):
        dis_cols = get_table_columns(cur, "diseases")
        # detect usable cols
        dis_key = choose(dis_cols, "disease_code", "disease", "name")
        dis_desc = choose(dis_cols, "disease_description", "description")
        if not dis_key:
            print("❌ 'diseases' table must have a name/code column (disease_code/disease/name).")
            sys.exit(1)

        frames = []
        if incidence_df is not None:
            cols = [c for c in ["DISEASE", "DISEASE_DESCRIPTION"] if c in incidence_df.columns]
            if cols:
                frames.append(incidence_df[cols].rename(columns={
                    "DISEASE": "DISEASE",
                    "DISEASE_DESCRIPTION": "DISEASE_DESCRIPTION"
                }))
        if reported_df is not None:
            cols = [c for c in ["DISEASE", "DISEASE_DESCRIPTION"] if c in reported_df.columns]
            if cols:
                frames.append(reported_df[cols].rename(columns={
                    "DISEASE": "DISEASE",
                    "DISEASE_DESCRIPTION": "DISEASE_DESCRIPTION"
                }))
        if frames:
            diseases_df = pd.concat(frames).drop_duplicates()
            print(f"✅ Upserting {len(diseases_df)} diseases...")
            # build insert
            if dis_desc:
                sql = f"INSERT IGNORE INTO diseases ({dis_key}, {dis_desc}) VALUES (%s, %s)"
            else:
                sql = f"INSERT IGNORE INTO diseases ({dis_key}) VALUES (%s)"
            for _, r in diseases_df.iterrows():
                key_val = normalize_str(r.get("DISEASE"))
                desc_val = normalize_str(r.get("DISEASE_DESCRIPTION"))
                if not key_val:
                    continue
                if dis_desc:
                    cur.execute(sql, (key_val, desc_val))
                else:
                    cur.execute(sql, (key_val,))
            cnx.commit()

    # ---- vaccines master ----
    if coverage_df is not None:
        vac_cols = get_table_columns(cur, "vaccines")
        vac_key = choose(vac_cols, "vaccine_code", "vaccine_name", "vaccine")
        vac_desc = choose(vac_cols, "vaccine_description", "description")
        if not vac_key:
            print("❌ 'vaccines' table must have a name/code column (vaccine_code/vaccine_name/vaccine).")
            sys.exit(1)

        needed = [c for c in ["ANTIGEN", "ANTIGEN_DESCRIPTION"] if c in coverage_df.columns]
        if needed:
            vdf = coverage_df[needed].drop_duplicates()
            print(f"✅ Upserting {len(vdf)} vaccines...")
            # pick best source for key
            def pick_vac_key(row):
                # prefer code if available
                return normalize_str(row.get("ANTIGEN")) or normalize_str(row.get("ANTIGEN_DESCRIPTION"))
            def pick_vac_desc(row):
                return normalize_str(row.get("ANTIGEN_DESCRIPTION")) or normalize_str(row.get("ANTIGEN"))
            if vac_desc:
                sql = f"INSERT IGNORE INTO vaccines ({vac_key}, {vac_desc}) VALUES (%s, %s)"
            else:
                sql = f"INSERT IGNORE INTO vaccines ({vac_key}) VALUES (%s)"
            for _, r in vdf.iterrows():
                key_val = pick_vac_key(r)
                if not key_val:
                    continue
                if vac_desc:
                    cur.execute(sql, (key_val, pick_vac_desc(r)))
                else:
                    cur.execute(sql, (key_val,))
            cnx.commit()

    # Refresh master maps
    # countries
    c_cols = get_table_columns(cur, "countries")
    c_id = choose(c_cols, "country_id", "id")
    c_iso = choose(c_cols, "iso_code", "iso3", "code", "iso_3_code")
    if not (c_id and c_iso):
        print("❌ countries table must have id & iso_code (or synonyms).")
        sys.exit(1)
    cur.execute(f"SELECT {c_id}, {c_iso} FROM countries")
    country_map = {str(k).strip().upper(): i for i, k in cur.fetchall() if k is not None}

    # vaccines
    v_cols = get_table_columns(cur, "vaccines")
    v_id = choose(v_cols, "vaccine_id", "id")
    v_key_code = choose(v_cols, "vaccine_code")
    v_key_name = choose(v_cols, "vaccine_name", "vaccine")
    vac_code_map = {}
    vac_name_map = {}
    if v_id:
        if v_key_code:
            cur.execute(f"SELECT {v_id}, {v_key_code} FROM vaccines")
            for i, k in cur.fetchall():
                if k is not None:
                    vac_code_map[str(k).strip().upper()] = i
        if v_key_name:
            cur.execute(f"SELECT {v_id}, {v_key_name} FROM vaccines")
            for i, k in cur.fetchall():
                if k is not None:
                    vac_name_map[str(k).strip().upper()] = i

    # diseases
    d_cols = get_table_columns(cur, "diseases")
    d_id = choose(d_cols, "disease_id", "id")
    d_key_code = choose(d_cols, "disease_code")
    d_key_name = choose(d_cols, "disease", "name")
    dis_code_map = {}
    dis_name_map = {}
    if d_id:
        if d_key_code:
            cur.execute(f"SELECT {d_id}, {d_key_code} FROM diseases")
            for i, k in cur.fetchall():
                if k is not None:
                    dis_code_map[str(k).strip().upper()] = i
        if d_key_name:
            cur.execute(f"SELECT {d_id}, {d_key_name} FROM diseases")
            for i, k in cur.fetchall():
                if k is not None:
                    dis_name_map[str(k).strip().upper()] = i

    # ===== FACT INSERTS =====

    # ---- coverage_data ----
    if coverage_df is not None and "coverage_data" in {t for t in ["coverage_data"]}:
        cov_cols = get_table_columns(cur, "coverage_data")
        f_id = choose(cov_cols, "id")
        f_country = choose(cov_cols, "country_id")
        f_vaccine = choose(cov_cols, "vaccine_id")
        f_year = choose(cov_cols, "year")
        f_covcat = choose(cov_cols, "coverage_category")
        f_covcat_desc = choose(cov_cols, "coverage_category_description")
        f_target = choose(cov_cols, "target_number")
        f_doses = choose(cov_cols, "doses")
        f_coverage = choose(cov_cols, "coverage")

        cols = [f_country, f_vaccine, f_year, f_covcat, f_covcat_desc, f_target, f_doses, f_coverage]
        cols = [c for c in cols if c]  # only existing cols
        placeholders = ", ".join(["%s"] * len(cols))
        sql_cov = f"INSERT INTO coverage_data ({', '.join(cols)}) VALUES ({placeholders})"

        inserted, skipped = 0, 0
        for _, r in coverage_df.iterrows():
            iso = normalize_str(r.get("CODE"))
            vac_code = normalize_str(r.get("ANTIGEN"))
            vac_desc = normalize_str(r.get("ANTIGEN_DESCRIPTION"))

            country_id = country_map.get((iso or "").upper())
            vaccine_id = None
            if vac_code:
                vaccine_id = vac_code_map.get(vac_code.upper())
            if not vaccine_id and vac_desc:
                vaccine_id = vac_name_map.get(vac_desc.upper())

            if not (country_id and vaccine_id):
                skipped += 1
                continue

            vals = []
            for c in cols:
                if c == f_country:
                    vals.append(country_id)
                elif c == f_vaccine:
                    vals.append(vaccine_id)
                elif c == f_year:
                    vals.append(to_int(r.get("YEAR")))
                elif c == f_covcat:
                    vals.append(normalize_str(r.get("COVERAGE_CATEGORY")))
                elif c == f_covcat_desc:
                    vals.append(normalize_str(r.get("COVERAGE_CATEGORY_DESCRIPTION")))
                elif c == f_target:
                    vals.append(to_int(r.get("TARGET_NUMBER")))
                elif c == f_doses:
                    vals.append(to_int(r.get("DOSES")))
                elif c == f_coverage:
                    vals.append(to_dec(r.get("COVERAGE")))
            try:
                cur.execute(sql_cov, tuple(vals))
                inserted += 1
            except Exception:
                skipped += 1
        cnx.commit()
        print(f"✅ coverage_data: inserted {inserted}, skipped {skipped}")

    # ---- incidence_rate_data ----
    if incidence_df is not None:
        inc_cols = get_table_columns(cur, "incidence_rate_data")
        f_country = choose(inc_cols, "country_id")
        f_disease = choose(inc_cols, "disease_id")
        f_year = choose(inc_cols, "year")
        f_denom = choose(inc_cols, "denominator")
        f_rate = choose(inc_cols, "incidence_rate")

        cols = [f_country, f_disease, f_year, f_denom, f_rate]
        cols = [c for c in cols if c]
        sql = f"INSERT INTO incidence_rate_data ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"

        inserted, skipped = 0, 0
        for _, r in incidence_df.iterrows():
            iso = normalize_str(r.get("CODE"))
            disease_code_or_name = normalize_str(r.get("DISEASE"))

            country_id = country_map.get((iso or "").upper())
            disease_id = dis_code_map.get((disease_code_or_name or "").upper()) \
                         or dis_name_map.get((disease_code_or_name or "").upper())

            if not (country_id and disease_id):
                skipped += 1
                continue

            vals = []
            for c in cols:
                if c == f_country: vals.append(country_id)
                elif c == f_disease: vals.append(disease_id)
                elif c == f_year: vals.append(to_int(r.get("YEAR")))
                elif c == f_denom: vals.append(normalize_str(r.get("DENOMINATOR")))
                elif c == f_rate: vals.append(to_dec(r.get("INCIDENCE_RATE")))
            try:
                cur.execute(sql, tuple(vals))
                inserted += 1
            except Exception:
                skipped += 1
        cnx.commit()
        print(f"✅ incidence_rate_data: inserted {inserted}, skipped {skipped}")

        # ---- reported_cases_data ----
    if reported_df is not None:
        rep_cols = get_table_columns(cur, "reported_cases_data")
        f_country = choose(rep_cols, "country_id")
        f_disease = choose(rep_cols, "disease_id")
        f_year = choose(rep_cols, "year")
        f_cases = choose(rep_cols, "cases", "reported_cases")

        cols = [f_country, f_disease, f_year, f_cases]
        cols = [c for c in cols if c]
        sql = f"INSERT INTO reported_cases_data ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"

        inserted, skipped = 0, 0
        for _, r in reported_df.iterrows():
            iso = normalize_str(r.get("CODE"))
            disease_name = normalize_str(r.get("DISEASE"))

            country_id = country_map.get((iso or "").upper())
            disease_id = dis_code_map.get((disease_name or "").upper()) \
                         or dis_name_map.get((disease_name or "").upper())

            # Skip rows where FK mismatch
            if not (country_id and disease_id):
                skipped += 1
                continue

            vals = []
            for c in cols:
                if c == f_country: 
                    vals.append(country_id)
                elif c == f_disease: 
                    vals.append(disease_id)
                elif c == f_year: 
                    vals.append(to_int(r.get("YEAR")))
                elif c == f_cases: 
                    vals.append(to_int(r.get("CASES")))

            try:
                cur.execute(sql, tuple(vals))
                inserted += 1
            except Exception as e:
                print(f"❌ Skipped row {r.to_dict()} due to error: {e}")
                skipped += 1

        cnx.commit()
        print(f"✅ reported_cases_data: inserted {inserted}, skipped {skipped}")

   

    # ---- vaccine_schedule_data ----
    if schedule_df is not None:
        sch_cols = get_table_columns(cur, "vaccine_schedule_data")
        f_country = choose(sch_cols, "country_id")
        f_vaccine = choose(sch_cols, "vaccine_id")
        f_year = choose(sch_cols, "year")
        f_rounds = choose(sch_cols, "schedulerounds")
        f_tpop = choose(sch_cols, "targetpop")
        f_tpopd = choose(sch_cols, "targetpop_description")
        f_geo = choose(sch_cols, "geoarea", "geo_area", "geo")
        f_age = choose(sch_cols, "ageadministered", "age_administered", "age")
        f_src = choose(sch_cols, "sourcecomment", "source_comment", "source")

        cols = [c for c in [f_country, f_vaccine, f_year, f_rounds, f_tpop, f_tpopd, f_geo, f_age, f_src] if c]
        sql = f"INSERT INTO vaccine_schedule_data ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"

        inserted, skipped = 0, 0
        for _, r in schedule_df.iterrows():
            iso = normalize_str(r.get("ISO_3_CODE"))
            country_id = country_map.get((iso or "").upper())

            # vaccine via code first, else by description
            vcode = normalize_str(r.get("VACCINECODE"))
            vdesc = normalize_str(r.get("VACCINE_DESCRIPTION"))
            vaccine_id = None
            if vcode:
                vaccine_id = vac_code_map.get(vcode.upper())
            if not vaccine_id and vdesc:
                vaccine_id = vac_name_map.get(vdesc.upper())

            if not (country_id and vaccine_id):
                skipped += 1
                continue

            vals = []
            for c in cols:
                if c == f_country: vals.append(country_id)
                elif c == f_vaccine: vals.append(vaccine_id)
                elif c == f_year: vals.append(to_int(r.get("YEAR")))
                elif c == f_rounds: vals.append(normalize_str(r.get("SCHEDULEROUNDS")))
                elif c == f_tpop: vals.append(normalize_str(r.get("TARGETPOP")))
                elif c == f_tpopd: vals.append(normalize_str(r.get("TARGETPOP_DESCRIPTION")))
                elif c == f_geo: vals.append(normalize_str(r.get("GEOAREA")))
                elif c == f_age: vals.append(normalize_str(r.get("AGEADMINISTERED")))
                elif c == f_src: vals.append(normalize_str(r.get("SOURCECOMMENT")))
            try:
                cur.execute(sql, tuple(vals))
                inserted += 1
            except Exception:
                skipped += 1
        cnx.commit()
        print(f"✅ vaccine_schedule_data: inserted {inserted}, skipped {skipped}")

    cur.close()
    cnx.close()
    print("\n🎉 Done. If any rows were skipped, the counts above tell you where and why (usually missing FK matches).")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Unexpected error:\n", e)
        traceback.print_exc()
        sys.exit(1)
