import pandas as pd
import requests
import time
from urllib.parse import quote

input_file = "animated_shows_with_wiki.csv"
output_file = "animated_shows_with_wiki_retry.csv"

df = pd.read_csv(input_file, encoding='latin-1')

HEADERS = {
    "User-Agent": "KousalyaSimilarityProject/1.0 (educational project)"
}

def fetch_wikipedia_summary(title, max_retries=5):
    if pd.isna(title) or str(title).strip() == "":
        return None, None, None, None, "missing_title"

    clean_title = str(title).strip()
    encoded_title = quote(clean_title, safe="")
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded_title}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=20)

            if response.status_code == 200:
                data = response.json()

                wiki_title = data.get("title")
                wiki_short_description = data.get("description")
                wiki_summary = data.get("extract")

                wiki_url = None
                if "content_urls" in data and "desktop" in data["content_urls"]:
                    wiki_url = data["content_urls"]["desktop"].get("page")

                return wiki_title, wiki_short_description, wiki_summary, wiki_url, "success"

            elif response.status_code == 429:
                wait_time = 2 ** attempt   # 1, 2, 4, 8, 16...
                print(f"429 for '{title}' -> waiting {wait_time} sec")
                time.sleep(wait_time)

            else:
                return None, None, None, None, f"failed_{response.status_code}"

        except Exception as e:
            wait_time = 2 ** attempt
            print(f"Error for '{title}' -> {e} -> waiting {wait_time} sec")
            time.sleep(wait_time)

    return None, None, None, None, "failed_after_retries"

# retry only rows that were not successful
mask = df["wiki_status"] != "success"
failed_indices = df[mask].index.tolist()

print(f"Retrying {len(failed_indices)} failed rows...")

for count, idx in enumerate(failed_indices, start=1):
    title = df.at[idx, "Title"]

    wiki_title, short_desc, summary, wiki_url, status = fetch_wikipedia_summary(title)

    df.at[idx, "wiki_title"] = wiki_title
    df.at[idx, "wiki_short_description"] = short_desc
    df.at[idx, "wiki_summary"] = summary
    df.at[idx, "wiki_url"] = wiki_url
    df.at[idx, "wiki_status"] = status

    if count % 25 == 0:
        print(f"Retried {count}/{len(failed_indices)} rows")

    # polite pause between rows
    time.sleep(1.0)

df.to_csv(output_file, index=False)

print("Done!")
print(df["wiki_status"].value_counts(dropna=False))