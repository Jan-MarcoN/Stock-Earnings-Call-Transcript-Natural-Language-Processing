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
# preprocessed_path = output_dir / f"preprocessed_with_prices.xlsx"
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
print(f"Saved {len(df_results)} matched sentences to {output_path}")

# --- Save filtered outputs ---
# Framing in coordination context
df_framing_coord = df_results[
    (df_results["coordination_term_count"] > 0) &
    (df_results["framing"].str.contains("opportunity|risk", case=False, na=False))
]
df_framing_coord.to_excel(output_dir / f"framing_coordination_{dt.date.today()}.xlsx", index=False)
framing_coord_output_path = output_dir / f"framing_coordination_{dt.date.today()}.xlsx"
print(f"Saved {len(df_framing_coord)} framing-coordination rows to {framing_coord_output_path}")

# --- Save filtered outputs for all uncertainty subtypes (one Excel, multiple sheets) ---
uncertainty_subtype_keys = list(uncertainty_subtypes.keys())

cooccurrence_path = output_dir / f"cooccurrence_coordination_all_{dt.date.today()}.xlsx"
with pd.ExcelWriter(cooccurrence_path) as writer:
    for subtype in uncertainty_subtype_keys:
        coord_df = df_results[(df_results[subtype] > 0) & (df_results["coordination_term_count"] > 0)]
        coord_df.to_excel(writer, sheet_name=subtype[:31], index=False)
        print(f"Added {len(coord_df)} rows for {subtype} to {cooccurrence_path}")
print(f"✅ Saved all co-occurrence outputs to {cooccurrence_path}")


#
# --- Analyze trends post-extraction ---
#
# Group 1: Company × Uncertainty Subtype Count
df_results["uncertainty_sum"] = df_results[uncertainty_subtype_keys].sum(axis=1)
df_sub_all = df_results.groupby("company")[uncertainty_subtype_keys + ["uncertainty_sum"]].sum().reset_index()
df_sub_all.columns = ["company"] + [f"{col}_count" for col in uncertainty_subtype_keys] + ["uncertainty_sum_count"]
df_company_summary = df_sub_all

# Group 2: Year × Framing × Uncertainty Subtype
df_results["year"] = pd.to_datetime(df_results["date"], errors="coerce").dt.year
yearly_frames = []
for subtype in uncertainty_subtype_keys:
    df_temp = df_results.copy()
    df_temp["total_sentences"] = 1
    df_temp[f"{subtype}_flag"] = df_temp[subtype] > 0
    df_temp["framing_opportunity"] = df_temp["framing"].str.contains("opportunity", case=False, na=False).astype(int)
    df_temp["framing_risk"] = df_temp["framing"].str.contains("risk", case=False, na=False).astype(int)
    df_group = df_temp.groupby("year")[
        [subtype, "framing_opportunity", "framing_risk", "total_sentences"]
    ].sum().reset_index()
    df_group.columns = [
        "year",
        f"{subtype}",
        f"{subtype}_framing_opportunity",
        f"{subtype}_framing_risk",
        "total_sentences"
    ]
    yearly_frames.append(df_group)

df_year_summary = yearly_frames[0]
for df_sub in yearly_frames[1:]:
    df_year_summary = df_year_summary.merge(df_sub, on=["year", "total_sentences"], how="outer").fillna(0)

# --- Normalization and ratios ---
for subtype in uncertainty_subtype_keys:
    df_year_summary[f"{subtype}_normalized"] = df_year_summary[subtype] / df_year_summary["total_sentences"]
    framing_sum = (
        df_year_summary[f"{subtype}_framing_opportunity"] + df_year_summary[f"{subtype}_framing_risk"]
    )
    df_year_summary[f"{subtype}_opportunity_ratio"] = (
        df_year_summary[f"{subtype}_framing_opportunity"] / framing_sum.replace(0, 1)
    )
    df_year_summary[f"{subtype}_risk_ratio"] = (
        df_year_summary[f"{subtype}_framing_risk"] / framing_sum.replace(0, 1)
    )

# Group 3: Co-occurrence of each subtype with framing types
all_cooccurrence_frames = []
for subtype in uncertainty_subtype_keys:
    df_results[f"{subtype}_flag"] = df_results[subtype] > 0
    co_df = df_results[
        df_results[f"{subtype}_flag"] & (df_results["framing"].str.contains("opportunity|risk", case=False, na=False))
    ].groupby(["company", "framing"]).size().reset_index(name="count")
    co_df["uncertainty_type"] = subtype
    all_cooccurrence_frames.append(co_df)

df_cooccurrence_all = pd.concat(all_cooccurrence_frames, ignore_index=True)

# Save extended trend summary
with pd.ExcelWriter(output_dir / f"uncertainty_trend_summary_{dt.date.today()}.xlsx") as writer:
    df_company_summary.to_excel(writer, sheet_name="Company_Uncertainty_Counts", index=False)
    # Ensure total_sentences is included in export
    df_year_summary.to_excel(writer, sheet_name="Yearly_Uncertainty_Framing", index=False)
    df_cooccurrence_all.to_excel(writer, sheet_name="Uncertainty_Framing_Cooccurrence", index=False)
print(f"Saved extended trend summary to {output_dir / f'uncertainty_trend_summary_{dt.date.today()}.xlsx'}")

print("✅ Tagging and analysis complete.")
print("✅ Trend analysis summaries saved.")

import plotly.graph_objs as go
from plotly.subplots import make_subplots
import plotly.io as pio

fig = make_subplots(rows=3, cols=1, shared_xaxes=True, subplot_titles=[
    "Raw Uncertainty Mentions by Type Over Time",
    "Opportunity Framing Ratio by Uncertainty Type",
    "Risk Framing Ratio by Uncertainty Type"
])

for subtype in uncertainty_subtype_keys:
    fig.add_trace(go.Scatter(
        x=df_year_summary["year"], y=df_year_summary[subtype],
        mode='lines', name=subtype), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df_year_summary["year"], y=df_year_summary[f"{subtype}_opportunity_ratio"],
        mode='lines', name=subtype), row=2, col=1)

    fig.add_trace(go.Scatter(
        x=df_year_summary["year"], y=df_year_summary[f"{subtype}_risk_ratio"],
        mode='lines', name=subtype), row=3, col=1)

fig.update_layout(height=900, title_text="Uncertainty Framing Trends Over Time")
html_path = output_dir / f"uncertainty_framing_trends_{dt.date.today()}.html"
pio.write_html(fig, file=html_path, auto_open=False)

print(f"✅ Framing trend HTML saved to {html_path}")