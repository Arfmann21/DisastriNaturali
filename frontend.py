import streamlit as st
from backend import query

st.title("Disastri Naturali")
st.subheader("""
Analisi di dati per l'uragano Harvey
""")


basic_filters, advancedButton, runQuery = st.columns([0.7, 0.3, 0.3])

basic_filters_container = basic_filters.container(border=True) 
basic_filters_container
basic_filters_container.markdown("Test")
advancedButton.markdown("Test 2")
runQuery.markdown("Test3")
if st.button("Mostra utenti verificati"):
    st.write("Risultati della query:")
    
    # Esegui la query su PySpark
    result = query(label=['relevant_information'])
    
    # Mostra i risultati
    for row in result:
        st.write(row)
