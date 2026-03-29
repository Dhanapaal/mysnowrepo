import json
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root

st.set_page_config(page_title="PDF RAG Search", layout="wide")
st.title("PDF RAG on Azure Blob PDFs")
st.caption("Search and answer over OCR'd PDFs indexed in Cortex Search")

session = get_active_session()
root = Root(session)

DB = "PDF_RAG_DB"
SCHEMA = "APP"
SERVICE = "PDF_CHUNKS_SEARCH"

search_service = (
    root.databases[DB]
        .schemas[SCHEMA]
        .cortex_search_services[SERVICE]
)

question = st.text_area("Ask a question", height=120, placeholder="e.g. What do the PDFs say about vaccine cold-chain handling?")
top_k = st.slider("Top chunks", 3, 10, 5)

if st.button("Search / Answer", type="primary") and question.strip():
    with st.spinner("Retrieving relevant chunks..."):
        resp = search_service.search(
            query=question,
            columns=["CHUNK_TEXT", "RELATIVE_PATH", "LAST_MODIFIED"],
            limit=top_k
        )

        resp_json = json.loads(resp.to_json())
        results = resp_json.get("results", [])

    if not results:
        st.warning("No matching chunks found.")
    else:
        st.subheader("Retrieved context")
        context_blocks = []
        for i, r in enumerate(results, start=1):
            path = r.get("RELATIVE_PATH", "")
            lm = r.get("LAST_MODIFIED", "")
            chunk = r.get("CHUNK_TEXT", "")

            with st.expander(f"{i}. {path}"):
                st.write(f"Last modified: {lm}")
                st.write(chunk)

            context_blocks.append(f"[Source {i}: {path}]\n{chunk}")

        rag_context = "\n\n".join(context_blocks)

        prompt = f"""
You are answering only from the retrieved PDF chunks below.
If the answer is not supported by the context, say you do not see enough evidence in the indexed PDFs.
Cite the source file names inline like [Source 1], [Source 2].

Question:
{question}

Context:
{rag_context}
"""

        with st.spinner("Generating answer..."):
            answer_df = session.sql(f"""
                SELECT AI_COMPLETE(
                    'snowflake-arctic',
                    $$ {prompt} $$
                ) AS ANSWER
            """).collect()

            answer = answer_df[0]["ANSWER"]

        st.subheader("Answer")
        st.write(answer)