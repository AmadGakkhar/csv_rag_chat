from utils.upload_file import UploadFile
import threading
import shutil


# Required Libraries
import sys
import asyncio
import streamlit as st
import os
from typing import List, Tuple
from utils.load_config import LoadConfig
from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from operator import itemgetter
from sqlalchemy import create_engine
from langchain_community.agent_toolkits import create_sql_agent
import langchain
import subprocess
from prepare_csv_xlsx_sqlitedb import prep_sql_instance
from prefinal import scrape_facebook_marketplace, locations
from OU_scrape import run_scraper as scrape_OU
from OU_scrape import zip_codes_to_test, num_listings_to_scrape
from AT_scraper import main as scrape_AT
from AT_scraper import zip_codes

langchain.debug = True
APPCFG = LoadConfig()


class ChatBot:
    """
    A ChatBot class capable of responding to messages using different modes of operation.
    It can interact with SQL databases, leverage language chain agents for Q&A,
    and use embeddings for Retrieval-Augmented Generation (RAG) with ChromaDB.
    """

    @staticmethod
    def respond(
        chatbot: List, message: str, chat_type: str, app_functionality: str
    ) -> Tuple:
        """
        Respond to a message based on the given chat and application functionality types.

        Args:
            chatbot (List): A list representing the chatbot's conversation history.
            message (str): The user's input message to the chatbot.
            chat_type (str): Describes the type of the chat (interaction with SQL DB or RAG).
            app_functionality (str): Identifies the functionality for which the chatbot is being used (e.g., 'Chat').

        Returns:
            Tuple[str, List, Optional[Any]]: A tuple containing an empty string, the updated chatbot conversation list,
                                             and an optional 'None' value. The empty string and 'None' are placeholder
                                             values to match the required return type and may be updated for further functionality.
                                             Currently, the function primarily updates the chatbot conversation list.
        """
        if app_functionality == "Chat":
            # If we want to use langchain agents for Q&A with our SQL DBs that was created from .sql files.
            if chat_type == "Q&A with stored SQL-DB":
                # directories
                if os.path.exists(APPCFG.sqldb_directory):
                    db = SQLDatabase.from_uri(f"sqlite:///{APPCFG.sqldb_directory}")
                    execute_query = QuerySQLDataBaseTool(db=db)
                    write_query = create_sql_query_chain(APPCFG.langchain_llm, db)
                    answer_prompt = PromptTemplate.from_template(
                        APPCFG.agent_llm_system_role
                    )
                    answer = answer_prompt | APPCFG.langchain_llm | StrOutputParser()
                    chain = (
                        RunnablePassthrough.assign(query=write_query).assign(
                            result=itemgetter("query") | execute_query
                        )
                        | answer
                    )
                    response = chain.invoke({"question": message})

                else:
                    chatbot.append(
                        (
                            message,
                            f"SQL DB does not exist. Please first create the 'sqldb.db'.",
                        )
                    )
                    return "", chatbot, None
            # If we want to use langchain agents for Q&A with our SQL DBs that were created from CSV/XLSX files.
            elif (
                chat_type == "Q&A with Uploaded CSV/XLSX SQL-DB"
                or chat_type == "Q&A with stored CSV/XLSX SQL-DB"
            ):
                if chat_type == "Q&A with Uploaded CSV/XLSX SQL-DB":
                    if os.path.exists(APPCFG.uploaded_files_sqldb_directory):
                        engine = create_engine(
                            f"sqlite:///{APPCFG.uploaded_files_sqldb_directory}"
                        )
                        db = SQLDatabase(engine=engine)
                        print(db.dialect)
                    else:
                        chatbot.append(
                            (
                                message,
                                f"SQL DB from the uploaded csv/xlsx files does not exist. Please first upload the csv files from the chatbot.",
                            )
                        )
                        return "", chatbot, None
                elif chat_type == "Q&A with stored CSV/XLSX SQL-DB":
                    if os.path.exists(APPCFG.stored_csv_xlsx_sqldb_directory):
                        engine = create_engine(
                            f"sqlite:///{APPCFG.stored_csv_xlsx_sqldb_directory}"
                        )
                        db = SQLDatabase(engine=engine)
                    else:
                        chatbot.append(
                            (
                                message,
                                f"SQL DB from the stored csv/xlsx files does not exist. Please first execute `src/prepare_csv_xlsx_sqlitedb.py` module.",
                            )
                        )
                        return "", chatbot, None
                print(db.dialect)
                print(db.get_usable_table_names())
                agent_executor = create_sql_agent(
                    APPCFG.langchain_llm, db=db, agent_type="openai-tools", verbose=True
                )
                response = agent_executor.invoke({"input": message})
                response = response["output"]

            elif chat_type == "RAG with stored CSV/XLSX ChromaDB":
                response = APPCFG.azure_openai_client.embeddings.create(
                    input=message, model=APPCFG.embedding_model_name
                )
                query_embeddings = response.data[0].embedding
                vectordb = APPCFG.chroma_client.get_collection(
                    name=APPCFG.collection_name
                )
                results = vectordb.query(
                    query_embeddings=query_embeddings, n_results=APPCFG.top_k
                )
                prompt = f"User's question: {message} \n\n Search results:\n {results}"

                messages = [
                    {"role": "system", "content": str(APPCFG.rag_llm_system_role)},
                    {"role": "user", "content": prompt},
                ]
                llm_response = APPCFG.azure_openai_client.chat.completions.create(
                    model=APPCFG.model_name, messages=messages
                )
                response = llm_response.choices[0].message.content

            # Get the `response` variable from any of the selected scenarios and pass it to the user.
            chatbot.append((message, response))
            return "", chatbot
        else:
            pass


# Function to generate response
def generate_response(
    text, chat_type="Q&A with stored CSV/XLSX SQL-DB", app_functionality="Chat"
):
    (
        _,
        response,
    ) = ChatBot.respond(
        chatbot=[],
        message=text,
        chat_type=chat_type,
        app_functionality=app_functionality,
    )
    # Replace this with your actual chatbot logic
    return response


# Main function


execution_started = False
execution_stopped = False

st.title("Scrape and Chat")

if "task" not in st.session_state:
    st.session_state.task = None
if "stop_event" not in st.session_state:
    st.session_state.stop_event = asyncio.Event()


async def start_fbmp_function():
    st.session_state.stop_event.clear()
    st.session_state.task = asyncio.create_task(
        scrape_facebook_marketplace(locations, st.session_state.stop_event)
    )


async def start_OU_function():
    st.session_state.stop_event.clear()
    st.session_state.task = asyncio.create_task(
        scrape_OU(zip_codes_to_test, num_listings_to_scrape)
    )


async def start_AT_function():
    st.session_state.stop_event.clear()
    st.session_state.task = asyncio.create_task(scrape_AT(zip_codes))


def stop_async_function():
    if st.session_state.task:
        st.session_state.stop_event.set()
        st.session_state.task.cancel()
        st.session_state.task = None


col1, col2, col3, col4, col5 = st.columns(5)
col6, col7, col8 = st.columns([1, 2, 1])

with col1:
    if st.button("Facebook Marketplace Scraper"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(start_fbmp_function())
        loop.run_forever()

with col2:
    if st.button("OU Scraper"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(start_OU_function())
        loop.run_forever()

with col3:
    if st.button("Autotraders Scraper"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(start_AT_function())
        loop.run_forever()

with col4:
    if st.button("Cancel Scraping"):
        stop_async_function()

with col5:
    if st.button("Create a DB"):

        prep_sql_instance.run_pipeline()
        st.write("DB Created")

with col7:

    if st.button("Clear All CSV Files"):
        ## Delete all the files in data/csv_xlsx and data/csv_xlsx_sqldb.db
        folder = "data/csv_xlsx"
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                st.write("CSVs Deleted")

            except Exception as e:
                st.write(f"Failed to delete {file_path}. Reason: {e}")
with col8:
    if st.button("Delete DB"):
        # Delete the data/csv_xlsx_sqldb.db file
        db_file = "data/csv_xlsx_sqldb.db"
        try:
            if os.path.exists(db_file):
                os.remove(db_file)
                st.write("Database Deleted")

        except Exception as e:
            st.write(f"Failed to delete {db_file}. Reason: {e}")


# Chat input form
with st.form("chat_form"):
    text_input = st.text_input("Type your message:")
    submit_button = st.form_submit_button(label="Send")

    if submit_button:
        # Generate response
        response = generate_response(text_input)
        st.write(response)
