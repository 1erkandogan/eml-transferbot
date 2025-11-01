import requests
from bs4 import BeautifulSoup
import pandas as pd

def get_data():
    url = "https://www.emajorleague.com/tournaments/league_transfers/34"
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table")  # adjust selector if needed

    # extract rows
    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        row = []
        for cell in cells:
            link = cell.find("a")
            if link:
                text = link.get_text(strip=True)
                href = link.get("href")
                row.append({"text": text, "href": href})
            else:
                row.append({"text": cell.get_text(strip=True), "href": None})
        rows.append(row)

    # build DataFrame
    max_cols = max(len(r) for r in rows)
    data = []
    for r in rows:
        data.append([c["text"] for c in r] + [c["href"] for c in r])

    cols = [f"col{i+1}" for i in range(max_cols)] + [f"href{i+1}" for i in range(max_cols)]
    df = pd.DataFrame(data, columns=cols)

    return df