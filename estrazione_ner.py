import pandas as pd
from collections import Counter
from transformers import pipeline

# 1. Inizializza il modello NER di Hugging Face
ner_pipeline = pipeline(
    "ner",
    model="dbmdz/bert-large-cased-finetuned-conll03-english",
    aggregation_strategy="simple"
)

# 2. Carica il dataset (assicurati che il percorso sia corretto)
df = pd.read_json('output/dataset.json')

# 3. Funzione per estrarre entità e il loro tipo da una riga di testo
def extract_entities(text):
    # Ottieni le entità dal modello
    ents = ner_pipeline(text)
    # Restituisci una lista di tuple (entità, tipo)
    return [(ent['word'], ent['entity_group']) for ent in ents]

# 4. Applica la funzione su ogni riga della colonna 'text'
df['entities'] = df['text'].apply(extract_entities)

# 5. Estrai tutte le entità (con il loro tipo) in un'unica lista
all_entities = [entity for sublist in df['entities'] for entity in sublist]

# 6. Conta le occorrenze per ciascuna coppia (Entity, Type)
entity_counts = Counter(all_entities)

# 7. Crea un DataFrame con i conteggi e il tipo di entità
entity_counts_df = pd.DataFrame(
    [(ent, typ, count) for ((ent, typ), count) in entity_counts.items()],
    columns=['Entity', 'Type', 'Count']
)

# 8. Salva i risultati in un file CSV
entity_counts_df.to_csv('entity_counts.csv', index=False)

print("Estrazione completata e salvata in 'entity_counts.csv'.")
