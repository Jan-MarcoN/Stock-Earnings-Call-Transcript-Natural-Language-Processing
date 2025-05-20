import pandas as pd
from pathlib import Path
import datetime as dt
import json

def load_dict(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# --- Setup paths ---
OUT_ROOT = Path("outputs")
output_dir = OUT_ROOT / str(dt.date.today())
output_dir.mkdir(parents=True, exist_ok=True)

# --- Load preprocessed data ---
preprocessed_path = output_dir / f"preprocessed_sentences_{dt.date.today()}.xlsx"
df = pd.read_excel(preprocessed_path)

# --- Load keyword dictionaries ---
hydrogen_dict = load_dict(Path("dictionaries/hydrogen_keywords.json"))
uncertainty_dict = load_dict(Path("dictionaries/uncertainty_keywords.json"))
framing_dict = load_dict(Path("dictionaries/framing_keywords.json"))
orchestration_keywords = load_dict(Path("dictionaries/orchestration_keywords.json"))

# --- Prepare flat lists ---
hydrogen_keywords = set(i.lower() for v in hydrogen_dict.values() for i in v)
general_uncertainty_keywords = set(uncertainty_dict["uncertainty_general"])
certainty_keywords = set(uncertainty_dict["certainty"])
uncertainty_subtypes = {
    k: set(v) for k, v in uncertainty_dict["uncertainty_subtypes"].items()
}

# --- Tagging ---
results = []
for _, row in df.iterrows():
    context = " ".join([str(row["context_before"]), str(row["sentence"]), str(row["context_after"])]).lower()

    matched_hydrogen = [kw for kw in hydrogen_keywords if kw in context]
    matched_certainty = [kw for kw in certainty_keywords if kw in context]
    matched_uncertainty_general = [kw for kw in general_uncertainty_keywords if kw in context]
    matched_coordination = [kw for kw in orchestration_keywords if kw in context]

    matched_uncertainty_subtypes = {
        subtype: [kw for kw in kw_list if kw in context]
        for subtype, kw_list in uncertainty_subtypes.items()
    }
    matched_subtypes = [k for k, v in matched_uncertainty_subtypes.items() if v]

    matched_framing = [cat for cat, words in framing_dict.items() if any(w in context for w in words)]

    results.append({
        "company": row["company"],
        "date": row["date"],
        "section": row["section"],
        "sentence": row["sentence"],
        "context_before": row["context_before"],
        "context_after": row["context_after"],

        # Tag counts
        "certainty_terms": len(matched_certainty),
        "uncertainty_general": len(matched_uncertainty_general),
        "technological": len(matched_uncertainty_subtypes["technological"]),
        "market_application": len(matched_uncertainty_subtypes["market_application"]),
        "ecosystem": len(matched_uncertainty_subtypes["ecosystem"]),
        "business_model": len(matched_uncertainty_subtypes["business_model"]),
        "interaction": len(matched_uncertainty_subtypes["interaction"]),
        "uncertainty_subtypes": len(matched_subtypes),
        "coordination_term_count": len(matched_coordination),
        "framing_count": len(matched_framing),

        # Optional: string matches (deactivated)
        # "certainty_terms_text": ", ".join(matched_certainty),
        # "uncertainty_general_text": ", ".join(matched_uncertainty_general),
        # "technological_text": ", ".join(matched_uncertainty_subtypes["technological"]),
        # ...
        "coordination_terms": ", ".join(matched_coordination),
        "framing": ", ".join(matched_framing),
        "keywords": ", ".join(matched_hydrogen)
    })

# --- Save full output ---
df_results = pd.DataFrame(results)
output_path = output_dir / f"hydrogen_matches_{dt.date.today()}.xlsx"
df_results.to_excel(output_path, index=False)

# --- Save filtered outputs ---
# Ecosystem + coordination co-occurrence
df_ecosystem_coord = df_results[(df_results["ecosystem"] > 0) & (df_results["coordination_term_count"] > 0)]
df_ecosystem_coord.to_excel(output_dir / f"cooccurrence_ecosystem_coordination_{dt.date.today()}.xlsx", index=False)

# Framing in coordination context
df_framing_coord = df_results[
    (df_results["coordination_term_count"] > 0) &
    (df_results["framing"].str.contains("opportunity|risk", case=False, na=False))
]
df_framing_coord.to_excel(output_dir / f"framing_coordination_{dt.date.today()}.xlsx", index=False)


# --- Analyze trends post-extraction ---

# Group 1: Company × Ecosystem Uncertainty Count
group_company_ecosystem = df_results.groupby("company")["ecosystem"].sum().reset_index()
group_company_ecosystem.columns = ["company", "ecosystem_uncertainty_count"]

# Group 2: Year × Ecosystem Framing (opportunity/risk)
df_results["year"] = pd.to_datetime(df_results["date"], errors="coerce").dt.year
df_results["framing_opportunity"] = df_results["framing"].str.contains("opportunity", case=False, na=False).astype(int)
df_results["framing_risk"] = df_results["framing"].str.contains("risk", case=False, na=False).astype(int)

group_year_framing = df_results.groupby("year")[["ecosystem", "framing_opportunity", "framing_risk"]].sum().reset_index()

# Group 3: Co-occurrence of ecosystem with framing types
df_results["ecosystem_flag"] = df_results["ecosystem"] > 0
df_results["framing_opportunity_flag"] = df_results["framing"].str.contains("opportunity", case=False, na=False)
df_results["framing_risk_flag"] = df_results["framing"].str.contains("risk", case=False, na=False)

cooccurrence_summary = df_results[
    df_results["ecosystem_flag"] & (df_results["framing_opportunity_flag"] | df_results["framing_risk_flag"])
].groupby(["company", "framing"]).size().reset_index(name="count")

# Save to Excel
with pd.ExcelWriter(output_dir / f"ecosystem_trend_summary_{dt.date.today()}.xlsx") as writer:
    group_company_ecosystem.to_excel(writer, sheet_name="Company_Ecosystem_Count", index=False)
    group_year_framing.to_excel(writer, sheet_name="Year_Ecosystem_Framing", index=False)
    cooccurrence_summary.to_excel(writer, sheet_name="Ecosystem_Framing_Cooccurrence", index=False)

print("✅ Tagging and analysis complete.")
print("✅ Trend analysis summaries saved.")