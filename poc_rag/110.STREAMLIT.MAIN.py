import json
import uuid
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# ============================================================
# CONFIG
# ============================================================
DB = "PDF_RAG_DB"
SCHEMA = "APP"
SEARCH_SERVICE = f"{DB}.{SCHEMA}.PDF_CHUNKS_SEARCH"
INSIGHTS_TABLE = f"{DB}.{SCHEMA}.PDF_RAG_APP_INSIGHTS"
INSIGHTS_DAILY_VIEW = f"{DB}.{SCHEMA}.PDF_RAG_APP_INSIGHTS_DAILY"

# ============================================================
# PAGE SETUP
# ============================================================
st.set_page_config(page_title="PDF RAG App", layout="wide")
st.title("PDF RAG App")
st.caption("Search your indexed PDF chunks, summarize them, and answer questions using Cortex.")

session = get_active_session()

# ============================================================
# HELPERS
# ============================================================
def esc(value: str) -> str:
    if value is None:
        return ""
    return value.replace("'", "''")


def run_search(question: str, top_k: int):
    payload = {
        "query": question,
        "columns": ["CHUNK_TEXT", "RELATIVE_PATH", "LAST_MODIFIED"],
        "limit": top_k
    }

    sql = f"""
    SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        '{SEARCH_SERVICE}',
        '{esc(json.dumps(payload))}'
    ) AS RESULT
    """

    raw = session.sql(sql).collect()[0]["RESULT"]

    if isinstance(raw, str):
        return json.loads(raw)

    return raw


def summarize_chunk(chunk_text: str) -> str:
    prompt = f"""
Summarize this retrieved PDF chunk in 2-3 short bullet points.
Be concise and factual.

Chunk:
{chunk_text}
"""

    sql = f"""
    SELECT AI_COMPLETE(
        'snowflake-arctic',
        '{esc(prompt)}'
    ) AS SUMMARY
    """

    return session.sql(sql).collect()[0]["SUMMARY"]


def answer_from_context(question: str, context_blocks: list[str]) -> str:
    prompt = f"""
Answer the user's question only from the retrieved context below.
If the answer is not supported by the context, say:
"I do not have enough evidence in the indexed PDFs."
Cite sources like [Source 1], [Source 2].

Question:
{question}

Context:
{chr(10).join(context_blocks)}
"""

    sql = f"""
    SELECT AI_COMPLETE(
        'snowflake-arctic',
        '{esc(prompt)}'
    ) AS ANSWER
    """

    return session.sql(sql).collect()[0]["ANSWER"]


def ensure_insights_objects():
    session.sql(f"""
    CREATE TABLE IF NOT EXISTS {INSIGHTS_TABLE} (
        EVENT_TS            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        EVENT_DATE          DATE DEFAULT CURRENT_DATE(),
        SESSION_ID          STRING,
        USER_NAME           STRING,
        QUERY_TEXT          STRING,
        TOP_K               NUMBER,
        RESULT_COUNT        NUMBER,
        SEARCH_REQUEST_ID   STRING,
        FIRST_SOURCE        STRING,
        ANSWER_CHARS        NUMBER
    )
    """).collect()

    session.sql(f"""
    CREATE OR REPLACE VIEW {INSIGHTS_DAILY_VIEW} AS
    SELECT
        EVENT_DATE,
        COUNT(*) AS QUERY_COUNT,
        COUNT(DISTINCT USER_NAME) AS USER_COUNT,
        AVG(RESULT_COUNT) AS AVG_RESULT_COUNT
    FROM {INSIGHTS_TABLE}
    GROUP BY EVENT_DATE
    ORDER BY EVENT_DATE
    """).collect()


def log_usage(session_id: str, question: str, top_k: int, result_count: int, request_id: str, first_source: str, answer_text: str):
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
        '{esc(session_id)}',
        CURRENT_USER(),
        '{esc(question)}',
        {top_k},
        {result_count},
        '{esc(request_id)}',
        '{esc(first_source)}',
        {len(answer_text)}
    """
    session.sql(sql).collect()


def load_usage_chart():
    sql = f"""
    SELECT EVENT_DATE, QUERY_COUNT
    FROM {INSIGHTS_DAILY_VIEW}
    ORDER BY EVENT_DATE
    """
    return session.sql(sql).to_pandas()


# ============================================================
# INIT
# ============================================================
ensure_insights_objects()

if "app_session_id" not in st.session_state:
    st.session_state["app_session_id"] = str(uuid.uuid4())

if "history" not in st.session_state:
    st.session_state["history"] = []

# ============================================================
# INPUT AREA
# ============================================================
left, right = st.columns([4, 1])

with left:
    question = st.text_input("Ask a question about your PDFs")

with right:
    top_k = st.selectbox("Top K", [1, 2, 3, 4, 5, 6, 7, 8], index=2)

submit = st.button("Submit", use_container_width=True)

# ============================================================
# MAIN ACTION
# ============================================================
if submit:
    if not question or not question.strip():
        st.warning("Please enter a question.")
    else:
        question = question.strip()

        st.markdown("### Your question")
        st.write(question)

        with st.spinner("Searching matching chunks..."):
            search_data = run_search(question, top_k)

        results = search_data.get("results", [])
        request_id = search_data.get("request_id", "")
        first_source = results[0].get("RELATIVE_PATH", "") if results else ""

        if not results:
            st.info("No matching chunks found.")
            final_answer = "No matching chunks found."
        else:
            st.markdown("## Retrieved chunks and summaries")

            context_blocks = []

            for i, row in enumerate(results, start=1):
                chunk_text = row.get("CHUNK_TEXT", "")
                relative_path = row.get("RELATIVE_PATH", "")
                last_modified = row.get("LAST_MODIFIED", "")

                with st.expander(f"Chunk {i}: {relative_path}", expanded=(i == 1)):
                    st.markdown("**Chunk text**")
                    st.write(chunk_text)

                    with st.spinner(f"Summarizing chunk {i}..."):
                        chunk_summary = summarize_chunk(chunk_text)

                    st.markdown("**Chunk summary**")
                    st.write(chunk_summary)

                    st.caption(f"Last modified: {last_modified}")

                context_blocks.append(f"[Source {i}: {relative_path}]\n{chunk_text}")

            with st.spinner("Generating final answer..."):
                final_answer = answer_from_context(question, context_blocks)

            st.markdown("## Final answer")
            st.write(final_answer)

        log_usage(
            session_id=st.session_state["app_session_id"],
            question=question,
            top_k=top_k,
            result_count=len(results),
            request_id=request_id,
            first_source=first_source,
            answer_text=final_answer
        )

        st.session_state["history"].append({
            "question": question,
            "answer": final_answer,
            "result_count": len(results)
        })

# ============================================================
# USAGE CHART
# ============================================================
st.divider()
st.markdown("## Usage chart")

try:
    usage_df = load_usage_chart()

    if not usage_df.empty:
        usage_df["EVENT_DATE"] = pd.to_datetime(usage_df["EVENT_DATE"])
        usage_df = usage_df.sort_values("EVENT_DATE").set_index("EVENT_DATE")

        st.line_chart(usage_df["QUERY_COUNT"])
        st.dataframe(usage_df.reset_index(), use_container_width=True)
    else:
        st.caption("No usage data yet.")
except Exception as ex:
    st.warning(f"Could not load usage chart: {ex}")

# ============================================================
# RECENT SESSION HISTORY
# ============================================================
st.divider()
st.markdown("## Recent session history")

if st.session_state["history"]:
    hist_df = pd.DataFrame(st.session_state["history"][::-1])
    st.dataframe(hist_df, use_container_width=True)
else:
    st.caption("No questions asked yet.")