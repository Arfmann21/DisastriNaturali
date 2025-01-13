import streamlit as st
from backend import query

st.write("""
# My first app
Hello *world!*
""")

if st.button("Mostra utenti verificati"):
    st.write("Risultati della query:")
    
    # Esegui la query su PySpark
    result = query()
    
    # Mostra i risultati
    for row in result:
        st.write(row)
