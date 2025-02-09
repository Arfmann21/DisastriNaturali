import streamlit as st
from backend import query, mean_label_polarity, mean_state_polarity, mean_polarity, df_ner
import pandas as pd
from streamlit_extras.stylable_container import stylable_container
import folium
from folium import Popup
from folium.plugins import MarkerCluster
import matplotlib.pyplot as plt
from streamlit_folium import folium_static  # Aggiungi questa importazione


#    df = df.withColumn("text", when(col("truncated"), col("full_text")).otherwise(col("text")))

# Inizializzazione di filtri
labels = None
verified = None
created_at = None
created_at_end = None
phase = None
place = None
possibly_sensitive = None

num_record = 0

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", 
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
aidr_labels = ["affected_individual", "caution_and_advice", "donation_and_volunteering", "infrastructure_and_utilities_damage", "injured_or_dead_people", "not_related_or_irrelevant", "personal", "relevant_information", "response_efforts", "sympathy_and_support"]
## Inizializzazione dello state
def initialize_state():
    if "selected_labels" not in st.session_state:
        st.session_state["selected_labels"] = []

    if "verified" not in st.session_state:
        st.session_state["verified"] = "Tutti"

    if "created_at" not in st.session_state:
        st.session_state["created_at"] = None
        
    if "created_at_end" not in st.session_state:
        st.session_state["created_at_end"] = None
        
    if "phase" not in st.session_state:
        st.session_state["phase"] = []

    if "place" not in st.session_state:
        st.session_state["place"] = [] 

    if "possibly_sensitive" not in st.session_state:
        st.session_state["possibly_sensitive"] = "Tutti"

    if "num_record" not in st.session_state:
        st.session_state["num_record"] = 0

    if "folium_place" not in st.session_state:
        st.session_state["folium_place"] = None

    if "folium_coordinates" not in st.session_state:
        st.session_state["folium_coordinates"] = None


def set_filters():
    global labels
    global verified
    global created_at
    global place
    global created_at_end
    global phase
    global possibly_sensitive
    global num_record

    labels = st.session_state["selected_labels"]
    verified = st.session_state["verified"]
    created_at = st.session_state["created_at"]
    created_at_end = st.session_state["created_at_end"]
    phase = st.session_state["phase"]
    possibly_sensitive = st.session_state["sensitive"]
    place = st.session_state["place"]

# Funzione per colorare le righe nella tabella
def color_rows(row):
    if row["sentiment_polarity"] < -0.3:
        return ['background-color: lightcoral; color: black'] * len(row)
    elif -0.3 <= row["sentiment_polarity"] <= 0.3:
        return ['background-color: lightcyan; color: black'] * len(row)
    else:
        return ['background-color: lightgreen; color: black'] * len(row)

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
    global verified
    global created_at
    global created_at_end
    global possibly_sensitive
    global place
    global phase
    result = query(labels=labels, verified=verified, created_at=created_at, created_at_end=created_at_end, place=place, possibly_sensitive=possibly_sensitive, phase = phase)

    st.session_state["num_record"] = len(result)
    st.session_state["result"] = result
    st.session_state["mean"] = []
    st.session_state["mean_label"] = []
    st.session_state["mean_state"] = []
    st.session_state["folium_place"] = None
    st.session_state["folium_coordinates"] = None

def main():
    st.set_page_config(layout="wide")
    initialize_state()

    st.title("Disastri Naturali")
    st.subheader("""
    Analisi di dati per l'uragano Harvey
    """)    

    # Aggiunge uno spazio vuoto
    st.markdown("###")

    # Colonne per i filtri di base, il pulsante per aprire i filtri avanzati e quello per eseguire la query
    draw_topbar()
    show_data()



def show_map(result):
# Ricrea la mappa con i dati esistenti

    place_col, coordinates_col = st.columns([0.3, 0.3], gap="medium", vertical_alignment="center")
    
    with place_col:
        __place_map(result)

    with coordinates_col:
        __coordinates_map(result)


def __coordinates_map(result):
    if st.session_state["folium_coordinates"] is None:
        m = folium.Map(location=[37.0902, -95.7129], zoom_start=4)
        for _, row in result.iterrows():
            if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
                color = get_color(row['sentiment_polarity'])

                popup_text = f"""
                        Sentiment: {row['sentiment_polarity']}<br>
                        Subjectivity: {row['sentiment_subjectivity']}<br>
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
        st.session_state["folium_coordinates"] = m
    else:
        m = st.session_state["folium_coordinates"]

    # Mostra la mappa
    st.markdown("### Mappa delle coordinate dei tweet")
    
    folium_static(m , width=500, height=400)

def __place_map(result):
    if st.session_state["folium_place"] is None:
        m = folium.Map(location=[37.0902, -95.7129], zoom_start=4)
        for _, row in result.iterrows():
            if pd.notna(row['place_latitude']) and pd.notna(row['place_longitude']):
                color = get_color(row['sentiment_polarity'])

                popup_text = f"""
                        Sentiment: {row['sentiment_polarity']}<br>
                        Subjectivity: {row['sentiment_subjectivity']}<br>
                        Place: {row['place_name']}
                        """
                    
                folium.CircleMarker(
                    location=[row['place_latitude'], row['place_longitude']],
                    radius=7,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.7,
                    popup = Popup(popup_text, max_width = 300)
                ).add_to(m)
        st.session_state["folium_place"] = m
    else:
        m = st.session_state["folium_place"]

    # Mostra la mappa
    st.markdown("### Mappa delle coordinate dei place")
    
    folium_static(m, width=500, height=400)

def draw_corr_pol_sub(result):
    st.markdown("### Correlazione tra sentiment polarity e subjectivity")
    
    st.scatter_chart(data = result, x = "sentiment_polarity", y = "sentiment_subjectivity", x_label = "Polarity", y_label = "Subjectivity", height=550, use_container_width=True)


def draw_corr_pol_aidr(result):
    st.markdown("### Correlazione tra sentiment polarity e label")

    st.scatter_chart(data = result, x = "aidr_label", y = "sentiment_polarity", x_label = "Label", y_label = "Polarity", height=550, use_container_width=True)

def draw_corr_pol_area(result):
    st.markdown("### Correlazione tra sentiment polarity e area")

    st.scatter_chart(data = result, x = "place_state", y = "sentiment_polarity", x_label = "State", y_label = "Polarity", height=550, use_container_width=True)

def show_ner_data():
    st.markdown("### Risultati Named Entity Recognition")
    st.dataframe(df_ner, column_order = ["entity", "type", "total_count"])

def draw_topbar():
    states_abbr = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

    global aidr_labels

    filters_col, runQuery = st.columns([0.8,0.12])

    with filters_col:
        # Colonne per i filtri base, una per ciascuno
        label_col, verified_col, created_at_col, place_col, possibly_sensitive_col, phase_col = st.columns([0.3, 0.25, 0.25, 0.3, 0.3, 0.3])

        with label_col:
            st.markdown("##### Label")
            st.multiselect("Collapsed", placeholder = "Nessuno selezionato", options = ["affected_individual", "caution_and_advice", "donation_and_volunteering",
                                                                                        "infrastructure_and_utility_damage", "injured_or_dead_people", "not_related_or_irrelevant",
                                                                                        "personal", "relevant_information", "response_efforts", "sympathy_and_support"], key ="selected_labels", label_visibility="collapsed")

        with verified_col:
            st.markdown("##### Verified")
            st.selectbox("Collapsed", options = ["Tutti", "True", "False"], key = "verified", label_visibility="collapsed")

        with created_at_col:
            st.markdown("##### Date")
            st.date_input(label="Collapsed", value= None, format="DD/MM/YYYY", key = "created_at", label_visibility="collapsed")
            st.date_input(label="Collapsed", value= None, format="DD/MM/YYYY", key = "created_at_end", label_visibility="collapsed")

        with place_col:
            st.markdown("##### Place")
            st.multiselect("Collapsed", states_abbr, label_visibility="collapsed", key = "place")

        with possibly_sensitive_col:
            st.markdown("##### Possibly sensitive")
            st.selectbox("Collapsed", options = ["Tutti", "True", "False"], key = "sensitive", placeholder = "Nessuno selezionato", label_visibility="collapsed")
            
        with phase_col:
            st.markdown("##### Phase")
            st.multiselect("Collapsed", options = ["Pre", "Durante", "Post"], label_visibility="collapsed", placeholder = "Nessuno selezionato", key = "phase")

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
                margin-right: 0pt;
                width: 100%;
                float: right;
            }""",
        ): 
            run_query = st.button("ESEGUI QUERY", type="secondary", use_container_width=True)

    if run_query:
        query_button()
        
        

def show_data():
    if "result" in st.session_state:
        result = st.session_state["result"]
        st.markdown("### Risultati della Query - " + str(st.session_state["num_record"]) + " record trovati")
        st.dataframe(result, use_container_width=True, column_order = ["aidr_label", "name", "screen_name", "date", "verified", "latitude", "longitude", "place_name", "place_latitude", "place_longitude", "possibly_sensitive", "sentiment_polarity", "sentiment_subjectivity", "favourite_count", "text", "phase"])
        
        if st.session_state["mean"] == []:
            global_mean = mean_polarity()
            
        else: global_mean = st.session_state["mean"]
        
        if global_mean[0] is not None:
            st.markdown(f"**SENTIMENT POLARITY E SUBJECTIVITY MEDIO:** <span style='color:{get_color(global_mean[0])};'>{global_mean[0]:.4f}</span>, {global_mean[1]:.4f}", unsafe_allow_html=True)
        else:
            st.markdown(f"**SENTIMENT POLARITY E SUBJECTIVITY MEDIO:** Nessun dato disponibile", unsafe_allow_html=True)

        show_map(result)
        show_ner_data()

        st.markdown("#####")
        corr_pol_sub_col, mean_pol_aidr = st.columns([ 0.4, 0.2])

        with corr_pol_sub_col:
            draw_corr_pol_aidr(result)

        with mean_pol_aidr:
            st.markdown("###")
            with stylable_container(
                key = "mean",
                css_styles = """
                    h6 
                    {
                        float: left;
                    }
                    """
            ):
                st.markdown("###### LABEL, POLARITY, SUBJECTIVITY")

                if st.session_state["mean_label"] != []:
                    means = st.session_state["mean_label"]

                else:
                    means = mean_label_polarity(aidr_labels)


                for i in range(0, len(aidr_labels)):
                    if(means[i][0] is not None):
                        st.markdown(f"**{aidr_labels[i]}:** <span style='color:{get_color(means[i][0])};'>{means[i][0]:.4f}</span>, {means[i][1]:.4f}", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{aidr_labels[i]}:** dati non disponibili", unsafe_allow_html=True)


        st.markdown("#####")
        draw_corr_pol_area(result)

        st.markdown("###")
        with stylable_container(
            key = "mean",
            css_styles = """
                h6 
                {
                    float: left;
                }
                """
        ):
            st.markdown("###### STATO, POLARITY, SUBJECTIVITY")

            if st.session_state["mean_state"] != []:
                means = st.session_state["mean_state"]

            else:
                means = mean_state_polarity(states)

            state_area_one, state_area_two, state_area_three, state_area_four, state_area_five = st.columns(5)
                
            with state_area_one:
                for i in range(0, 10):
                    if(means[i][0] is not None):
                        st.markdown(f"**{states[i]}:** <span style='color:{get_color(means[i][0])};'>{means[i][0]:.4f}</span>, {means[i][1]:.4f}", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{states[i]}:** dati non disponibili", unsafe_allow_html=True)
                    
            with state_area_two:
                for i in range(10, 20):
                    if(means[i][0] is not None):
                        st.markdown(f"**{states[i]}:** <span style='color:{get_color(means[i][0])};'>{means[i][0]:.4f}</span>, {means[i][1]:.4f}", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{states[i]}:** dati non disponibili", unsafe_allow_html=True)

            with state_area_three:
                for i in range(20, 30):
                    if(means[i][0] is not None):
                        st.markdown(f"**{states[i]}:** <span style='color:{get_color(means[i][0])};'>{means[i][0]:.4f}</span>, {means[i][1]:.4f}", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{states[i]}:** dati non disponibili", unsafe_allow_html=True)

            with state_area_four:
                for i in range(30, 40):
                    if(means[i][0] is not None):
                        st.markdown(f"**{states[i]}:** <span style='color:{get_color(means[i][0])};'>{means[i][0]:.4f}</span>, {means[i][1]:.4f}", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{states[i]}:** dati non disponibili", unsafe_allow_html=True)

            with state_area_five:
                for i in range(40, 50):
                    if(means[i][0] is not None):
                        st.markdown(f"**{states[i]}:** <span style='color:{get_color(means[i][0])};'>{means[i][0]:.4f}</span>, {means[i][1]:.4f}", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{states[i]}:** dati non disponibili", unsafe_allow_html=True)

        st.markdown("#####")
        draw_corr_pol_sub(result)

main()