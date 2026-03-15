import pandas as pd

# load your enriched dataset
df = pd.read_csv("animated_shows_with_wiki.csv")

# keep only rows that were NOT successful
retry_data = df[df["wiki_status"] != "success"].copy()

print("Rows remaining to retry:", len(retry_data))

# save to new file
retry_data.to_csv("animated_shows_retry.csv", index=False)

print("Saved retry dataset to animated_shows_retry.csv")