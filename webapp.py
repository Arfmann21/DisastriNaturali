import streamlit as st
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static  # Aggiungi questa importazione

# Carica il file JSON
json_file = 'output/dataset.json'
try:
    df = pd.read_json(json_file)
except ValueError as e:
    st.error(f"Errore nel caricamento del file JSON: {e}")
    st.stop()

# Modifica il testo basandoti su `truncated`
df["text"] = df.apply(
    lambda row: row["full_text"] if row["truncated"] else row["text"], axis=1
)

# Estrai il campo `sentiment.polarity`, `sentiment.subjectivity` e `city`, `coordinates`
df["sentiment_polarity"] = df["sentiment"].apply(lambda x: x.get("polarity") if pd.notnull(x) else None)

# Aggiungi direttamente la colonna "Oggettivo/Soggettivo" basandosi sul valore di `subjectivity`
df["oggettivita"] = df["sentiment"].apply(
    lambda x: "Oggettivo" if pd.notnull(x) and x.get("subjectivity", 0) <= 0.5 else "Soggettivo")

# Estrai la città e le coordinate
df['city'] = df['place'].apply(lambda x: x.get('full_name') if pd.notnull(x) and 'full_name' in x else None)
df['coordinates'] = df['place'].apply(lambda x: x.get('bounding_box', {}).get('coordinates') if pd.notnull(x) and 'bounding_box' in x else None)

# Estrai la prima coppia di coordinate (latitudine, longitudine)
def extract_lat_lon(coordinates):
    if isinstance(coordinates, list) and len(coordinates) > 0:
        first_point = coordinates[0][0] if isinstance(coordinates[0], list) else None
        if first_point and len(first_point) == 2:
            return first_point[1], first_point[0]
    return None, None

df['latitude'], df['longitude'] = zip(*df['coordinates'].apply(extract_lat_lon))

# Rimuovi le colonne non richieste
columns_to_drop = ["id", "place", "full_text", "truncated", "sentiment", "subjectivity"]
df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

# Funzione per determinare il colore in base al sentiment
def get_color(polarity):
    if polarity < -0.3:
        return 'red'  # Rosso per sentiment negativo
    elif -0.3 <= polarity <= 0.3:
        return 'blue'  # Blu per sentiment neutro
    else:
        return 'green'  # Verde per sentiment positivo

# Configura il layout della pagina
st.set_page_config(layout="wide", page_title="Disastri Naturali")

# Titolo e sottotitolo
st.title("Disastri Naturali")
st.subheader("Analisi dei dati per l'uragano Harvey")

# Funzione per colorare le righe nella tabella
def color_rows(row):
    if row["sentiment_polarity"] < -0.3:
        return ['background-color: lightcoral; color: black'] * len(row)
    elif -0.3 <= row["sentiment_polarity"] <= 0.3:
        return ['background-color: lightcyan; color: black'] * len(row)
    else:
        return ['background-color: lightgreen; color: black'] * len(row)

# Applica lo stile condizionale alla tabella
styled_df = df.style.apply(color_rows, axis=1)

# Visualizza la legenda dei colori
st.markdown("### Legenda dei Colori")
st.markdown(
    """
    - **🟥 Rosso (Negativo)**: Valore di polarità compreso tra -1 e -0.3.
    - **🟦 Blu (Neutro)**: Valore di polarità compreso tra -0.3 e 0.3.
    - **🟩 Verde (Positivo)**: Valore di polarità maggiore di 0.3.
    """
)

# Visualizza la tabella dei dati
st.markdown("### Tabella dei Dati")
st.dataframe(styled_df, use_container_width=True)

# Crea una mappa centrata sulle coordinate medie
m = folium.Map(location=[df['latitude'].mean(), df['longitude'].mean()], zoom_start=10)

# Crea un MarkerCluster per raggruppare i punti sulla mappa
marker_cluster = MarkerCluster().add_to(m)

# Aggiungi i marker alla mappa
for _, row in df.iterrows():
    if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
        color = get_color(row['sentiment_polarity'])
        tooltip = f"{row['text']} | {row['city']}"
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=7,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=tooltip
        ).add_to(marker_cluster)

# Visualizza la mappa sotto la tabella
st.markdown("### Mappa dei Punti di Coordinata")
folium_static(m)
