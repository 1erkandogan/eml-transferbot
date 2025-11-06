import pandas as pd

def transfer_message(df: pd.DataFrame) -> str:
    transfer_in = []
    transfer_out = []

    i_emj = ":white_check_mark:"
    o_emj = ":x:"
    a_emj = ":handshake:"
    d_emj = ":wave:"
    for _, row in df.iterrows():
        player = f"[{row['PLAYER']}]({row['PLAYER_LINK']})"
        club = row["CLUB"]
        date = row["DATE"].strftime("%Y-%m-%d %H:%M")
        ct = row["CONTRACT_TYPE"].strip().lower()
        if ct == "classic contract":
            transfer_in.append(f"{date} {player} {a_emj} {club}")
        elif ct == "contract cancel":
            transfer_out.append(f"{date} {player} {d_emj} {club}")

    parts = []
    if transfer_in:
        parts.append(f"{i_emj} | **Yeni Transferler**\n" + "\n".join(transfer_in))
    if transfer_out:
        parts.append(f"{o_emj} | **Takımdan Ayrılanlar**\n" + "\n".join(transfer_out))

    discord_message = "\n\n".join(parts)
    return discord_message