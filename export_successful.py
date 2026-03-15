import pandas as pd

# load your enriched dataset
df = pd.read_csv("animated_shows_with_wiki.csv")

# keep rows that have a summary
description_data = df[
    df["wiki_summary"].notna() &
    (df["wiki_summary"].astype(str).str.strip() != "")
].copy()

print("Rows with summaries:", len(description_data))

# export to new file
description_data.to_csv("description_data.csv", index=False)

print("Saved to description_data.csv")