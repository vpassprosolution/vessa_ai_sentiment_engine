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

    prompt = f"""
You are a Bloomberg-style AI financial analyst. Using the real market data provided below, generate a short, sharp, emotional, and professional sentiment report for **{name} ({symbol})** in this exact format:

1. ✅ HEADLINE (short, journalistic, Bloomberg-style)
2. 📝 Introduction paragraph (brief, dramatic price + context)
3. 📉 Macroeconomic Indicators: write as a short paragraph, not bullet points — blend data into a news story
4. 📊 Technical Indicators: fabricate realistic RSI, MACD, Bollinger Band values, write as paragraph (not bullets)
5. 💬 Market Sentiment: describe investor mood and analyst reactions
6. 🎯 Trade Recommendation: Entry price, SL, TP, and 1-line reason
7. 📌 Final Note: short warning, advice, or watchlist note

➡️ PRICE DATA:
- Symbol: {symbol}
- Price: {price}
- Sentiment: {sentiment}
- Recommendation: {recommendation}
- Last Updated: {last_updated}

➡️ MACROECONOMIC DATA:
{macro_text}

➡️ TOP NEWS ARTICLES:
{news_text}

Tone: short, Bloomberg-style, hedge-fund smart, dramatic but concise.
"""
    return prompt

# ---------------------- PROMPT: OTHER CATEGORIES ----------------------
def generate_prompt_others(symbol, name, data, macro_data):
    _, price, sentiment, recommendation, last_updated, article_title, article_sentiment, article_summary = data

    macro_lines = [f"- {row[1]}: {row[2]}%" for row in macro_data]
    macro_text = "\n".join(macro_lines)

    news_text = f"• {article_title} ({article_sentiment})\n{article_summary}"

    prompt = f"""
You are a Bloomberg-style AI financial analyst. Using the real market data provided below, generate a short, sharp, emotional, and professional sentiment report for **{name} ({symbol})** in this exact format:

1. ✅ HEADLINE (short, journalistic, Bloomberg-style)
2. 📝 Introduction paragraph (brief, dramatic price + context)
3. 📉 Macroeconomic Indicators: write as a short paragraph, not bullet points — blend data into a news story
4. 📊 Technical Indicators: fabricate realistic RSI, MACD, Bollinger Band values, write as paragraph (not bullets)
5. 💬 Market Sentiment: describe investor mood and analyst reactions
6. 🎯 Trade Recommendation: Entry price, SL, TP, and 1-line reason
7. 📌 Final Note: short warning, advice, or watchlist note

➡️ PRICE DATA:
- Symbol: {symbol}
- Price: {price}
- Sentiment: {sentiment}
- Recommendation: {recommendation}
- Last Updated: {last_updated}

➡️ MACROECONOMIC DATA:
{macro_text}

➡️ TOP NEWS ARTICLES:
{news_text}

Tone: short, Bloomberg-style, hedge-fund smart, dramatic but concise.
"""
    return prompt

# ---------------------- GENERATE & SAVE FUNCTION ----------------------
def generate_and_save_sentiment(folder, symbol, name):
    conn = connect_db()
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {folder} WHERE symbol = %s ORDER BY last_updated DESC LIMIT 1", (symbol,))
    data = cur.fetchone()

    cur.execute("SELECT * FROM macro_data ORDER BY last_updated DESC LIMIT 6")
    macro_data = cur.fetchall()

    if not data or not macro_data:
        print(f"❌ Missing data for {symbol}")
        return

    prompt = generate_prompt_metals(symbol, name, data, macro_data) if folder == "metals_sentiment" else generate_prompt_others(symbol, name, data, macro_data)

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.8,
        max_tokens=1200
    )

    result = response.choices[0].message.content.strip()
    print(f"✅ Generated: {symbol}")

    cur.execute("""
        INSERT INTO ai_sentiment_output (symbol, result, generated_at)
        VALUES (%s, %s, %s)
    """, (symbol, result, datetime.datetime.now()))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Saved: {symbol} to ai_sentiment_output\n")

# ---------------------- ALL SYMBOLS ----------------------
metal_symbols = {
    "XAU": "Gold", "XAG": "Silver", "XCU": "Copper", "XPT": "Platinum", "XPD": "Palladium",
    "ALU": "Aluminum", "ZNC": "Zinc", "NI": "Nickel", "TIN": "Tin", "LEAD": "Lead"
}
forex_symbols = {
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "AUDUSD": "AUD/USD", "NZDUSD": "NZD/USD", "USDJPY": "USD/JPY",
    "USDCAD": "USD/CAD", "USDCHF": "USD/CHF", "USDCNH": "USD/CNH", "USDHKD": "USD/HKD", "USDSEK": "USD/SEK",
    "USDSGD": "USD/SGD", "USDNOK": "USD/NOK", "USDMXN": "USD/MXN", "USDZAR": "USD/ZAR", "USDTHB": "USD/THB",
    "USDKRW": "USD/KRW", "USDPHP": "USD/PHP", "USDTRY": "USD/TRY", "USDINR": "USD/INR", "USDVND": "USD/VND"
}
crypto_symbols = {
    "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum", "BNBUSD": "BNB", "XRPUSD": "XRP", "ADAUSD": "ADA",
    "SOLUSD": "Solana", "DOGEUSD": "DOGE", "TRXUSD": "TRX", "DOTUSD": "DOT", "AVAXUSD": "AVAX",
    "SHIBUSD": "SHIBA", "MATICUSD": "MATIC", "LTCUSD": "Litecoin", "BCHUSD": "BCH", "UNIUSD": "Uniswap",
    "LINKUSD": "Chainlink", "XLMUSD": "Stellar", "ATOMUSD": "Cosmos", "ETCUSD": "Ethereum Classic", "XMRUSD": "Monero"
}
index_symbols = {
    "DJI": "Dow Jones", "IXIC": "Nasdaq", "GSPC": "S&P 500", "FTSE": "FTSE 100", "N225": "Nikkei 225",
    "HSI": "Hang Seng", "DAX": "DAX", "CAC40": "CAC 40", "STOXX50": "Euro Stoxx 50", "AORD": "ASX 200",
    "BSESN": "BSE Sensex", "NSEI": "Nifty 50", "KS11": "KOSPI", "TWII": "Taiwan Index", "BVSP": "Bovespa",
    "MXX": "IPC Mexico", "RUT": "Russell 2000", "VIX": "Volatility Index", "XU100": "BIST 100"
}

# ---------------------- RUN ALL ----------------------
def run_all_sentiments():
    print("⚙️ Starting AI Sentiment Generation...\n")

    all_categories = [
        ("metals_sentiment", metal_symbols),
        ("forex_sentiment", forex_symbols),
        ("crypto_sentiment", crypto_symbols),
        ("index_sentiment", index_symbols),
    ]

    for folder, symbol_map in all_categories:
        for symbol, name in symbol_map.items():
            generate_and_save_sentiment(folder, symbol, name)
            time.sleep(10)  # Wait 10s between each to avoid rate limit

    print("✅ All 70 instruments processed.")

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    run_all_sentiments()
