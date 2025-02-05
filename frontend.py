import streamlit as st
from backend import query
import pandas as pd
from streamlit_extras.stylable_container import stylable_container
import folium
from folium import Popup
from streamlit_folium import folium_static  # Aggiungi questa importazione


#    df = df.withColumn("text", when(col("truncated"), col("full_text")).otherwise(col("text")))

# Inizializzazione di filtri
labels = None
verified = None
created_at = None
place = None
num_record = 0

## Inizializzazione dello state
def initialize_state():
    if "selected_labels" not in st.session_state:
        st.session_state["selected_labels"] = []

    if "verified" not in st.session_state:
        st.session_state["verified"] = "Tutti"

    if "created_at" not in st.session_state:
        st.session_state["created_at"] = None

    if "place" not in st.session_state:
        st.session_state["place"] = "" 

    if "num_record" not in st.session_state:
        st.session_state["num_record"] = 0

def set_filters():
    global labels
    global verified
    global created_at
    global place
    global num_record

    labels = st.session_state["selected_labels"]
    verified = st.session_state["verified"]
    created_at = st.session_state["created_at"]
    place = st.session_state["place"]



def get_color(polarity):
    if polarity < -0.3:
        return 'red'  # Rosso per sentiment negativo
    elif -0.3 <= polarity <= 0.3:
        return 'blue'  # Blu per sentiment neutro
    else:
        return 'green'  # Verde per sentiment positivo

def query_button():
    set_filters()
    global labels
    result = query(labels=labels, verified=verified, created_at=created_at, place=place)

    st.session_state["num_record"] = len(result)
    st.session_state["result"] = result

def main():
    st.set_page_config(layout="wide")
    initialize_state()

    st.title("Disastri Naturali")
    st.subheader("""
    Analisi di dati per l'uragano Harvey
    """)    

    # Aggiunge uno spazio vuoto
    st.markdown("#")

    # Colonne per i filtri di base, il pulsante per aprire i filtri avanzati e quello per eseguire la query
    draw_topbar()

def draw_topbar():
    states_abbr = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

    basic_filters_col, advancedButton, runQuery = st.columns([0.7, 0.2, 0.2])

    with basic_filters_col:
        # Colonne per i filtri base, una per ciascuno
        label_col, verified_col, created_at_col, place_col = st.columns([0.2, 0.17, 0.3, 0.3])

        with label_col:
            st.markdown("##### Label")
            st.multiselect("Collapsed", placeholder = "Nessuno selezionato", options = ["injured_or_dead_people", "infrastructure_and_utility_damage", "caution_and_advice", 
                                        "donation_and_volunteering", "affected_individual", "missing_and_found_people", "sympathy_and_support", "personal", 
                                        "other_useful_information", "irrelevant_or_not_related"], key ="selected_labels", label_visibility="collapsed")

        with verified_col:
            st.markdown("##### Verified")
            st.selectbox("Collapsed", options = ["Tutti", "True", "False"], key = "verified", label_visibility="collapsed")

        with created_at_col:
            st.markdown("##### Created at")
            st.date_input(label="Collapsed", value= None, format="DD/MM/YYYY", key = "created_at", label_visibility="collapsed")

        with place_col:
            st.markdown("##### Place")
            st.multiselect("Collapsed", states_abbr, label_visibility="collapsed")

    with advancedButton:
        with stylable_container(
            "advanced",
            css_styles="""
            button {
                background-color: #96BEF3;
                color: white;
                border: none;
                height:50pt;  
                display: inline-block;
                max-width: 200pt;
                width: 100%;
                margin-left: 30pt;
                float: right;
            }

            .advanced {
            background-color: red;
            }
            
            h6 {
            float: right;
            }"""
        ): 
            st.markdown("###### 0 selezionati")
            st.button("FILTRI AVANZATI", type="secondary")

    with runQuery:
        with stylable_container(
            "green",
            css_styles="""
            button {
                background-color: #70D88F;
                color: white;
                border: none;
                height:50pt;  
                max-width: 200pt;
                width: 100%;
                float: right;
            }""",
        ): 
            st.markdown("######")
            run_query = st.button("ESEGUI QUERY", type="secondary")

    if run_query:
        query_button()

    if "result" in st.session_state:
        result = st.session_state["result"]

        st.markdown("### Risultati della Query - " + str(st.session_state["num_record"]) + " record trovati")
        st.dataframe(result, use_container_width=True, column_order = ["aidr_label", "name", "screen_name", "date", "verified", "latitude", "longitude", "place_name", "place_latitude", "place_longitude", "possibly_sensitive", "sentiment_polarity", "sentiment_subjectivity", "favourite_count", "text"])

        # Ricrea la mappa con i dati esistenti
        m = folium.Map(location=[37.0902, -95.7129], zoom_start=4)
        for _, row in result.iterrows():
            if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
                color = get_color(row['sentiment_polarity'])

                popup_text = f"""
                        Sentiment: {row['sentiment_polarity']}<br>
                        Subjectivity: {row['sentiment_subjectivity']}<br>
                        Place: {row['place_name']}
                        """
                
                folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=7,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.7,
                    popup = Popup(popup_text, max_width = 300)
                ).add_to(m)

        # Mostra la mappa
        st.markdown("### Mappa dei Punti di Coordinata")
        folium_static(m)

main()