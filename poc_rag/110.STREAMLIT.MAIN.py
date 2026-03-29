import json
import uuid
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# --------------------------------------------------
# Config
# --------------------------------------------------
DB = "PDF_RAG_DB"
SCHEMA = "APP"
SEARCH_SERVICE = f"{DB}.{SCHEMA}.PDF_CHUNKS_SEARCH"
INSIGHTS_TABLE = f"{DB}.{SCHEMA}.PDF_RAG_APP_INSIGHTS"
INSIGHTS_VIEW = f"{DB}.{SCHEMA}.PDF_RAG_APP_INSIGHTS_DAILY"

st.set_page_config(page_title="PDF RAG App", layout="wide")
st.title("PDF RAG App")
st.caption("Ask a question, review matching chunks, see chunk summaries, and track usage.")

session = get_active_session()

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def sql_escape(value: str) -> str:
    return value.replace("'", "''") if value else value

def run_search(question: str, top_k: int):
    payload = {
        "query": question,
        "columns": ["CHUNK_TEXT", "RELATIVE_PATH", "LAST_MODIFIED"],
        "limit": top_k
    }

    payload_sql = sql_escape(json.dumps(payload))

    sql = f"""
    SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        '{SEARCH_SERVICE}',
        '{payload_sql}'
    ) AS RESULT
    """
    raw = session.sql(sql).collect()[0]["RESULT"]

    if isinstance(raw, str):
        data = json.loads(raw)
    else:
        data = raw

    return data

def summarize_text(text: str) -> str:
    prompt = f"""
Summarize the following retrieved PDF chunk in 2-3 bullet points.
Keep it concise and factual.

Chunk:
{text}
"""
    prompt_sql = sql_escape(prompt)

    sql = f"""
    SELECT AI_COMPLETE(
        'snowflake-arctic',
        '{prompt_sql}'
    ) AS SUMMARY
    """
    return session.sql(sql).collect()[0]["SUMMARY"]

def log_usage(session_id: str, question: str, top_k: int, result_count: int, request_id: str, first_source: str, answer_text: str):
    question_sql = sql_escape(question)
    request_id_sql = sql_escape(request_id or "")
    first_source_sql = sql_escape(first_source or "")

    sql = f"""
    INSERT INTO {INSIGHTS_TABLE}
    (
        SESSION_ID,
        USER_NAME,
        QUERY_TEXT,
        TOP_K,
        RESULT_COUNT,
        SEARCH_REQUEST_ID,
        FIRST_SOURCE,
        ANSWER_CHARS
    )
    SELECT
        '{sql_escape(session_id)}',
        CURRENT_USER(),
        '{question_sql}',
        {top_k},
        {result_count},
        '{request_id_sql}',
        '{first_source_sql}',
        {len(answer_text)}
    """
    session.sql(sql).collect()

def get_usage_chart_df():
    sql = f"""
    SELECT EVENT_DATE, QUERY_COUNT
    FROM {INSIGHTS_VIEW}
    ORDER BY EVENT_DATE
    """
    return session.sql(sql).to_pandas()

# --------------------------------------------------
# Session state
# --------------------------------------------------
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

if "pending_question" not in st.session_state:
    st.session_state.pending_question = ""

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# --------------------------------------------------
# Input area
# st.chat_input is supported in Streamlit in Snowflake. :contentReference[oaicite:1]{index=1}
# --------------------------------------------------
new_msg = st.chat_input("Type your question here and press Enter")

if new_msg:
    st.session_state.pending_question = new_msg

left, right = st.columns([3, 1])
with left:
    top_k = st.slider("Top chunks", 1, 8, 3)
with right:
    submit = st.button("Submit", use_container_width=True)

if st.session_state.pending_question:
    st.info(f"Pending question: {st.session_state.pending_question}")

# --------------------------------------------------
# Main action
# --------------------------------------------------
if submit and st.session_state.pending_question.strip():
    question = st.session_state.pending_question.strip()

    with st.spinner("Searching matching chunks..."):
        search_data = run_search(question, top_k)

    results = search_data.get("results", [])
    request_id = search_data.get("request_id", "")
    first_source = results[0].get("RELATIVE_PATH", "") if results else ""

    # Display the user's message
    with st.chat_message("user"):
        st.write(question)

    # Build context + show chunks + summarize each chunk
    answer_context = []
    chunk_summaries = []

    with st.chat_message("assistant"):
        if not results:
            st.warning("No matching chunks found.")
            final_answer = "No matching chunks found."
        else:
            st.subheader("Retrieved chunks")
            for i, row in enumerate(results, start=1):
                chunk_text = row.get("CHUNK_TEXT", "")
                source = row.get("RELATIVE_PATH", "")
                modified = row.get("LAST_MODIFIED", "")

                with st.expander(f"Chunk {i} — {source}", expanded=(i == 1)):
                    st.markdown("**Chunk text**")
                    st.write(chunk_text)

                    with st.spinner(f"Summarizing chunk {i}..."):
                        chunk_summary = summarize_text(chunk_text)

                    st.markdown("**Chunk summary**")
                    st.write(chunk_summary)

                answer_context.append(f"[Source {i}: {source}]\n{chunk_text}")
                chunk_summaries.append(f"[Source {i}] {chunk_summary}")

            # Final answer from retrieved context
            final_prompt = f"""
Answer the question only from the retrieved PDF chunks below.
If the answer is not supported by the chunks, say that you do not have enough evidence in the indexed PDFs.
Cite sources like [Source 1].

Question:
{question}

Retrieved chunks:
{chr(10).join(answer_context)}

Chunk summaries:
{chr(10).join(chunk_summaries)}
"""
            final_prompt_sql = sql_escape(final_prompt)

            with st.spinner("Generating final answer..."):
                final_answer = session.sql(f"""
                    SELECT AI_COMPLETE(
                        'snowflake-arctic',
                        '{final_prompt_sql}'
                    ) AS ANSWER
                """).collect()[0]["ANSWER"]

            st.subheader("Final answer")
            st.write(final_answer)

    # Persist to insights
    log_usage(
        session_id=st.session_state.session_id,
        question=question,
        top_k=top_k,
        result_count=len(results),
        request_id=request_id,
        first_source=first_source,
        answer_text=final_answer
    )

    st.session_state.chat_history.append({
        "question": question,
        "answer": final_answer,
        "result_count": len(results)
    })

    st.session_state.pending_question = ""

# --------------------------------------------------
# Usage chart
# --------------------------------------------------
st.divider()
st.subheader("Usage chart")

try:
    chart_df = get_usage_chart_df()
    if not chart_df.empty:
        chart_df["EVENT_DATE"] = pd.to_datetime(chart_df["EVENT_DATE"])
        chart_df = chart_df.set_index("EVENT_DATE")
        st.line_chart(chart_df["QUERY_COUNT"])
        st.dataframe(chart_df.reset_index(), use_container_width=True)
    else:
        st.caption("No usage yet.")
except Exception as ex:
    st.warning(f"Could not load usage chart: {ex}")

# --------------------------------------------------
# Recent interactions
# --------------------------------------------------
st.divider()
st.subheader("Recent interactions in this session")

if st.session_state.chat_history:
    recent_df = pd.DataFrame(st.session_state.chat_history[::-1][:10])
    st.dataframe(recent_df, use_container_width=True)
else:
    st.caption("No questions submitted yet.")