from openai import OpenAI
import os
from dotenv import load_dotenv

# ================== LOAD ENVIRONMENT VARIABLES ==================
load_dotenv()

# ---------------- OPENAI CLIENT ----------------
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Safety check
if not os.getenv("OPENAI_API_KEY"):
    raise ValueError("OPENAI_API_KEY not found! Make sure your .env file exists and contains the key.")


class BiasEngine:
    def analyze(self, bd_texts, intl_texts, topic, start_date, end_date):
        # Prepare text blocks - 10 articles with 3000 characters each
        bd_block = "\n\n---\n\n".join(bd_texts[:10])
        intl_block = "\n\n---\n\n".join(intl_texts[:10])

        prompt = f"""You are an expert media bias analyst. Analyze ONLY the provided texts. Never use external knowledge.

**TOPIC:** {topic}
**TIME PERIOD:** {start_date} to {end_date}

**BANGLADESHI MEDIA COVERAGE:**
{bd_block}

**INTERNATIONAL MEDIA COVERAGE:**
{intl_block}

**Strict Rules - Follow exactly:**
- Base EVERY claim on explicit content in the texts above.
- When discussing language, framing, tone or bias, ALWAYS quote the exact phrase/sentence and mention which side/article it comes from.
- If one side has very few or no directly relevant articles, clearly state: "Limited or no relevant coverage found in Bangladeshi/International media for this specific event."
- If the provided texts do not give enough evidence for a clear difference or bias, explicitly write: "Insufficient evidence in the provided texts to determine a clear difference."
- Do not invent events, quotes, or word choices that are not present.

Perform a structured analysis in exactly these 5 steps:

1. **BD Narrative Summary**  
   Summarize the main narrative, key arguments, tone, and emphasis in Bangladeshi media. Support with direct quotes. If limited coverage, state it clearly.

2. **International Narrative Summary**  
   Summarize the main narrative, key arguments, tone, and emphasis in International media. Support with direct quotes.

3. **Framing Comparison**  
   Compare how the same events or facts are framed differently. Analyze language/word choice, what is emphasized or omitted, and perspective. Use specific quotes as evidence. If one side lacks relevant content, state so.

4. **Bias Detection**  
   Identify potential biases (emotional language, selective reporting, one-sided sourcing, nationalistic slant). Support every point with exact quotes from the texts.

5. **Overall Conclusion**  
   Give a balanced assessment:
   - Which side (if any) shows stronger bias and why, or state if evidence is insufficient.
   - Most significant framing differences (or state if differences are minor or one side has limited coverage).
   - Any notable patterns observed.

Use a neutral, academic tone throughout."""

        # OpenAI API call
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise, evidence-based media analyst. Never hallucinate facts or speculate beyond the given texts."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=2000,
            temperature=0.3
        )

        return response.choices[0].message.content