import streamlit as st

st.set_page_config(layout="wide")

st.title("Disastri Naturali")
st.subheader("""
Analisi di dati per l'uragano Harvey
""")

# Aggiunge uno spazio vuoto
st.markdown("#")

# Colonne per i filtri di base, il pulsante per aprire i filtri avanzati e quello per eseguire la query
basic_filters_col, advancedButton, runQuery = st.columns([0.7, 0.3, 0.3])

with basic_filters_col:
    # Colonne per i filtri base, una per ciascuno
    label, verified, created_at, test = st.columns([0.2, 0.1, 0.3, 0.3])

    with label:
        label_label = st.markdown("##### Label")
        st.multiselect("Collapsed",["Test1", "Test2"], key ="TEST", label_visibility="collapsed")

    with verified:
        verified_label = st.markdown("##### Verified")
        st.selectbox("Collapsed", ["Tutti", "True", "False"], label_visibility="collapsed")

advancedButton.markdown("Test 2")
runQuery.markdown("Test3")