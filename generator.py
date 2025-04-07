import psycopg2
import openai
import os
import datetime
import time
from dotenv import load_dotenv

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# ---------------------- DB CONNECTION ----------------------
def connect_db():
    return psycopg2.connect(
        host="interchange.proxy.rlwy.net",
        database="railway",
        user="postgres",
        password="vVMyqWjrqgVhEnwyFifTQxkDtPjQutGb",
        port="30451"
    )

# ---------------------- PROMPT: METALS ----------------------
def generate_prompt_metals(symbol, name, data, macro_data):
    _, price, sentiment, recommendation, last_updated, *articles = data
    macro_lines = [f"- {row[1]}: {row[2]}%" for row in macro_data]
    macro_text = "\n".join(macro_lines)

    article_blocks = []
    for i in range(0, len(articles), 3):
        if articles[i]:
            block = f"• {articles[i]} ({articles[i+1]})\n{articles[i+2]}"
            article_blocks.append(block)
    news_text = "\n\n".join(article_blocks)

    return build_prompt(symbol, name, price, sentiment, recommendation, last_updated, macro_text, news_text)

# ---------------------- PROMPT: SINGLE ARTICLE ----------------------
def generate_prompt_single(symbol, name, data, macro_data):
    _, price, sentiment, recommendation, last_updated, article_title, article_sentiment, article_summary = data
    macro_lines = [f"- {row[1]}: {row[2]}%" for row in macro_data]
    macro_text = "\n".join(macro_lines)
    news_text = f"• {article_title} ({article_sentiment})\n{article_summary}"
    return build_prompt(symbol, name, price, sentiment, recommendation, last_updated, macro_text, news_text)

# ---------------------- COMMON PROMPT FORMAT ----------------------
def build_prompt(symbol, name, price, sentiment, recommendation, last_updated, macro_text, news_text):
    return f"""
You are a Bloomberg-style AI financial analyst. Using the real market data provided below, generate a short, sharp, emotional, and professional sentiment report for **{name} ({symbol})** in this exact format:

1. ✅ HEADLINE (short, journalistic, Bloomberg-style)
2. 📝 Introduction paragraph (brief, dramatic price + context)
3. 📉 Macroeconomic Indicators: write as a short paragraph, not bullet points — blend data into a news story
4. 📊 Technical Indicators: fabricate realistic RSI, MACD, Bollinger Band values, write as paragraph (not bullets)
5. 💬 Market Sentiment: describe investor mood and analyst reactions
6. 🎯 Trade Recommendation: Entry price, SL, TP, and 1-line reason
7. 📌 Final Note: short warning, advice, or watchlist note

➞ PRICE DATA:
- Symbol: {symbol}
- Price: {price}
- Sentiment: {sentiment}
- Recommendation: {recommendation}
- Last Updated: {last_updated}

➞ MACROECONOMIC DATA:
{macro_text}

➞ TOP NEWS ARTICLES:
{news_text}

Tone: short, Bloomberg-style, hedge-fund smart, dramatic but concise.
"""

# ---------------------- GENERATE & SAVE ----------------------
def generate_and_save_sentiment(folder, symbol, name):
    conn = connect_db()
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {folder} WHERE TRIM(symbol) = %s ORDER BY last_updated DESC LIMIT 1", (symbol.strip(),))
    data = cur.fetchone()
    print(f"DEBUG: Fetched data for {symbol} ➜ {data}")

    cur.execute("SELECT * FROM macro_data ORDER BY last_updated DESC LIMIT 6")
    macro_data = cur.fetchall()
    print(f"DEBUG: Macro count for {symbol} ➜ {len(macro_data)}")

    if not data or not macro_data:
        print(f"❌ Data missing for {symbol}.")
        return

    if folder == "metals_sentiment":
        prompt = generate_prompt_metals(symbol, name, data, macro_data)
    else:
        prompt = generate_prompt_single(symbol, name, data, macro_data)

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.8,
        max_tokens=1200
    )

    result = response.choices[0].message.content.strip()
    print(f"✅ Sentiment generated for {symbol}")

    cur.execute("""
        DELETE FROM ai_sentiment_output
        WHERE symbol = %s AND generated_at::date = CURRENT_DATE
    """, (symbol,))
    conn.commit()

    cur.execute("""
        INSERT INTO ai_sentiment_output (symbol, result, generated_at)
        VALUES (%s, %s, %s)
    """, (symbol, result, datetime.datetime.now()))
    conn.commit()

    cur.close()
    conn.close()
    print(f"✅ {symbol} saved to ai_sentiment_output.\n")

# ---------------------- INSTRUMENT LIST ----------------------
# (same `all_instruments` you already have — keep unchanged)

# ---------------------- RUN ----------------------
if __name__ == "__main__":
    for folder, symbol, name in all_instruments:
        try:
            generate_and_save_sentiment(folder, symbol, name)
        except Exception as e:
            print(f"Error with {symbol}: {str(e)}")
        time.sleep(10)
