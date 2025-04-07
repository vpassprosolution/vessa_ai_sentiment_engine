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

    # 🧽 Clean symbol for DB matching
    symbol_clean = symbol.replace("/", "").strip()

    # ✅ Fetch latest price/news/sentiment data
    cur.execute(f"SELECT * FROM {folder} WHERE TRIM(symbol) = %s ORDER BY last_updated DESC LIMIT 1", (symbol_clean,))
    data = cur.fetchone()
    print(f"DEBUG: Fetched data for {symbol} ➜ {data}")

    # ✅ Fetch macroeconomic indicators
    cur.execute("SELECT * FROM macro_data ORDER BY last_updated DESC LIMIT 6")
    macro_data = cur.fetchall()
    print(f"DEBUG: Macro count for {symbol} ➜ {len(macro_data)}")

    # ❌ If no data, skip
    if not data or not macro_data:
        print(f"❌ Data missing for {symbol}.")
        return

    # ✅ Generate Prompt
    prompt = generate_prompt_metals(symbol, name, data, macro_data) if folder == "metals_sentiment" else generate_prompt_single(symbol, name, data, macro_data)

    # ✅ Generate Sentiment from OpenAI
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.8,
        max_tokens=1200
    )

    result = response.choices[0].message.content.strip()
    print(f"✅ Sentiment generated for {symbol}")

    # ✅ Delete existing record for today
    cur.execute("""
        DELETE FROM ai_sentiment_output
        WHERE symbol = %s AND generated_at::date = CURRENT_DATE
    """, (symbol,))
    conn.commit()

    # ✅ Insert the fresh new report
    cur.execute("""
        INSERT INTO ai_sentiment_output (symbol, result, generated_at)
        VALUES (%s, %s, %s)
    """, (symbol, result, datetime.datetime.now()))
    conn.commit()

    cur.close()
    conn.close()
    print(f"✅ {symbol} saved to ai_sentiment_output.\n")


# ---------------------- INSTRUMENT LIST ----------------------
all_instruments = [
    ("metals_sentiment", "XAU", "Gold"), ("metals_sentiment", "XAG", "Silver"), ("metals_sentiment", "XCU", "Copper"),
    ("metals_sentiment", "XPT", "Platinum"), ("metals_sentiment", "XPD", "Palladium"), ("metals_sentiment", "ALU", "Aluminum"),
    ("metals_sentiment", "ZNC", "Zinc"), ("metals_sentiment", "NI", "Nickel"), ("metals_sentiment", "TIN", "Tin"),
    ("metals_sentiment", "LEAD", "Lead"),

    ("forex_sentiment", "EUR/USD", "EUR/USD"), ("forex_sentiment", "GBP/USD", "GBP/USD"),
    ("forex_sentiment", "AUD/USD", "AUD/USD"), ("forex_sentiment", "NZD/USD", "NZD/USD"),
    ("forex_sentiment", "USD/JPY", "USD/JPY"), ("forex_sentiment", "USD/CAD", "USD/CAD"),
    ("forex_sentiment", "USD/CHF", "USD/CHF"), ("forex_sentiment", "USD/CNH", "USD/CNH"),
    ("forex_sentiment", "USD/HKD", "USD/HKD"), ("forex_sentiment", "USD/SEK", "USD/SEK"),
    ("forex_sentiment", "USD/SGD", "USD/SGD"), ("forex_sentiment", "USD/NOK", "USD/NOK"),
    ("forex_sentiment", "USD/MXN", "USD/MXN"), ("forex_sentiment", "USD/ZAR", "USD/ZAR"),
    ("forex_sentiment", "USD/THB", "USD/THB"), ("forex_sentiment", "USD/KRW", "USD/KRW"),
    ("forex_sentiment", "USD/PHP", "USD/PHP"), ("forex_sentiment", "USD/TRY", "USD/TRY"),
    ("forex_sentiment", "USD/INR", "USD/INR"), ("forex_sentiment", "USD/VND", "USD/VND"),

    ("crypto_sentiment", "BTC/USD", "Bitcoin"), ("crypto_sentiment", "ETH/USD", "Ethereum"),
    ("crypto_sentiment", "BNB/USD", "BNB"), ("crypto_sentiment", "XRP/USD", "XRP"),
    ("crypto_sentiment", "ADA/USD", "Cardano"), ("crypto_sentiment", "SOL/USD", "Solana"),
    ("crypto_sentiment", "DOGE/USD", "Dogecoin"), ("crypto_sentiment", "TRX/USD", "TRON"),
    ("crypto_sentiment", "DOT/USD", "Polkadot"), ("crypto_sentiment", "AVAX/USD", "Avalanche"),
    ("crypto_sentiment", "SHIB/USD", "SHIBA"), ("crypto_sentiment", "MATIC/USD", "Polygon"),
    ("crypto_sentiment", "LTC/USD", "Litecoin"), ("crypto_sentiment", "BCH/USD", "Bitcoin Cash"),
    ("crypto_sentiment", "UNI/USD", "Uniswap"), ("crypto_sentiment", "LINK/USD", "Chainlink"),
    ("crypto_sentiment", "XLM/USD", "Stellar"), ("crypto_sentiment", "ATOM/USD", "Cosmos"),
    ("crypto_sentiment", "ETC/USD", "Ethereum Classic"), ("crypto_sentiment", "XMR/USD", "Monero"),

    ("index_sentiment", "DJI", "Dow Jones"), ("index_sentiment", "IXIC", "Nasdaq"),
    ("index_sentiment", "GSPC", "S&P 500"), ("index_sentiment", "FTSE", "FTSE 100"),
    ("index_sentiment", "N225", "Nikkei 225"), ("index_sentiment", "HSI", "Hang Seng"),
    ("index_sentiment", "DAX", "DAX Germany"), ("index_sentiment", "CAC40", "CAC 40"),
    ("index_sentiment", "STOXX50", "STOXX 50"), ("index_sentiment", "AORD", "ASX 200"),
    ("index_sentiment", "BSESN", "BSE Sensex"), ("index_sentiment", "NSEI", "NSE Nifty"),
    ("index_sentiment", "KS11", "KOSPI"), ("index_sentiment", "TWII", "Taiwan Index"),
    ("index_sentiment", "BVSP", "Bovespa"), ("index_sentiment", "MXX", "IPC Mexico"),
    ("index_sentiment", "RUT", "Russell 2000"), ("index_sentiment", "VIX", "Volatility Index"),
    ("index_sentiment", "XU100", "BIST 100")
]

# ---------------------- RUN ----------------------
if __name__ == "__main__":
    for folder, symbol, name in all_instruments:
        try:
            generate_and_save_sentiment(folder, symbol, name)
        except Exception as e:
            print(f"Error with {symbol}: {str(e)}")
        time.sleep(10)
