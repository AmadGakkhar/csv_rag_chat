# csv_rag_chat

## How to run the Cahtbot

1. Clone the Reporsitory. Run the following command in the terminal.
        git clone https://github.com/AmadGakkhar/csv_rag_chat.git

2. Create a new environment. Install requirements.txt. And activate the environment.

3. In the root folcer create a .env file with the following structure.

        OPENAI_API_KEY = ""
        OPENAI_API_BASE = ""
        OPENAI_API_VERSION = ""

        FB_EMAIL = ""
        FB_PASSWORD = ""

4. In the root folder, run the following command.

        python -m streamlit run src/app_streamlit.py

5. The app will open up. Click on Delete DB and Remove CSV Files Button.

6. Run all the scrapers one by one. And at the end click "Create DB Button"

7. Once DB is created, you can use the chatbot to ask questions from your database.
