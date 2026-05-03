import pandas as pd
import re
import json
import dateutil.parser
from typing import Dict, Any, List

from langchain.chat_models import ChatOpenAI
from spellchecker import SpellChecker


class CrewAIStandardizationAgent:
    """
    Hybrid spelling correction (enterprise-safe, batched AI).

    - Spellchecker first
    - AI fallback in ONE batch call
    """

    def __init__(self, use_ai=True):
        self.spell = SpellChecker()
        self.use_ai = use_ai
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0) if use_ai else None #gpt-4, gpt-4o


    def ai_correct_words_batch(self, words: List[str]) -> Dict[str, str]:
        """
        Takes a list of words, returns {original: corrected}
        """

        if not self.llm or not words:
            return {w: w for w in words}

        prompt = f"""
You are a spelling correction assistant.

Fix ONLY spelling mistakes.
Do NOT rewrite meaning.
Do NOT expand abbreviations.
Return STRICT JSON mapping.

Input words:
{words}

Return ONLY valid JSON:
{{ "original_word": "corrected_word" }}
"""

        try:
            response = self.llm.predict(prompt)

            # ✅ safer JSON extraction (non-greedy)
            match = re.search(r"\{.*?\}", response, re.DOTALL)
            if not match:
                raise ValueError("No JSON found in LLM response")

            result = json.loads(match.group())
            return result

        except Exception as e:
            print("LLM parsing failed:", e)
            print("Raw response:", response if 'response' in locals() else "No response")
            return {w: w for w in words}


    def split_camel_case(self, text):
        import re
        text = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', text)  # camelCase
        text = re.sub(r'[_\-]+', ' ', text)               # snake_case / kebab-case
        text = text.lower()
        return text


    def correct_spelling_series(self, series: pd.Series) -> pd.Series:

        words_to_fix = set()

        for val in series.dropna():
            processed_val = self.split_camel_case(str(val)).strip()

            for word in processed_val.split():

                if word.isdigit() or len(word) <= 2:
                    continue

                if re.match(r"^[A-Z0-9\-_]+$", word) and any(c.isdigit() for c in word):
                    continue

                w = word.lower()

                # ✅ push long unknown tokens (like "washingmachine") to LLM
                if w not in self.spell:
                    words_to_fix.add(w)

        ai_corrections = self.ai_correct_words_batch(list(words_to_fix))


        def transform(val):
            if pd.isna(val):
                return val

            # ✅ apply SAME preprocessing here
            processed_val = self.split_camel_case(str(val)).strip()

            out = []
            for word in processed_val.split():

                key = word.lower()
                fixed = ai_corrections.get(key, word)

                # preserve casing
                if word.istitle():
                    fixed = fixed.title()
                elif word.isupper():
                    fixed = fixed.upper()

                out.append(fixed)

            return " ".join(out)

        return series.apply(transform)


class AIColumnAdvisor:
    """
    AI suggests column type + explanation.
    """

    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0) #gpt-4, gpt-4o

    def suggest_type(self, series: pd.Series, column_name: str) -> Dict[str, Any]:

        sample = series.dropna().head(15).astype(str).tolist()

        if not sample:
            return {
                "detected_type": "text",
                "confidence": 0,
                "reason": "Empty or null column"
            }

        prompt = f"""
You are a strict data classification assistant.

Column Name: {column_name}

Sample Values:
{sample}

Decide MOST likely type:

- date
- datetime
- phone
- email
- numeric
- id
- text

Rules:
- Pure numbers → numeric
- Only label as date if clear patterns exist
- If unsure → text

Return ONLY valid JSON:
{{
  "detected_type": "...",
  "confidence": 0-100,
  "reason": "why this type was chosen"
}}
"""

        response = self.llm.predict(prompt)

        # ✅ safer JSON extraction
        print(response)


        try:
            match = re.search(r"\{.*\}", response, re.DOTALL)
            return json.loads(match.group())
        except:
            return {
                "detected_type": "text",
                "confidence": 0,
                "reason": "AI parse failure"
            }




class DataStandardizerAI:

    def __init__(self, use_ai=True, confidence_threshold=75):

        self.use_ai = use_ai
        self.threshold = confidence_threshold

        self.ai_advisor = AIColumnAdvisor() if use_ai else None
        self.spelling_agent = CrewAIStandardizationAgent(use_ai=use_ai)

        # Explainability storage
        #self.explainability = {}
        #self.explainability: Dict[str, Any] = {}
        self.explainability = {}


    def get_response(self):
        return self.get_response
    

    def ai_advisor(self):
        return self.ai_advisor


    def detect_column_type_pattern(self, series: pd.Series) -> str:

        sample = series.dropna().head(50)
        if sample.empty:
            return "text"

        s = [str(v).strip() for v in sample]

        if sum("@" in v for v in s) / len(s) > 0.7:
            return "email"

        if sum(7 <= len(re.sub(r"\D", "", v)) <= 15 for v in s) / len(s) > 0.7:
            return "phone"

        try:
            numeric_ratio = sum(
                1 for v in s if float(v.replace(",", "").replace("$", ""))
            ) / len(s)
            if numeric_ratio > 0.8:
                return "numeric"
        except:
            pass

        if sum(bool(re.search(r"\d{4}-\d{2}-\d{2}", v)) for v in s) / len(s) > 0.5:
            return "date"

        if sum(bool(re.search(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}", v)) for v in s) / len(s) > 0.5:
            return "datetime"
        if sum(bool(re.search(r"\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}", v)) for v in s) / len(s) > 0.5:
            return "datetime"
        if sum(bool(re.search(r"\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}", v)) for v in s) / len(s) > 0.5:
            return "datetime"
        if sum(bool(re.search(r"\d{4}\d{2}\d{2}_\d{2}\d{2}", v)) for v in s) / len(s) > 0.5:
            return "datetime"


        return "text"


    def detect_column_type(self, series: pd.Series, column_name: str):

        pattern_type = self.detect_column_type_pattern(series)

        explain = {
            "pattern_type": pattern_type
        }

        if not self.use_ai:
            explain.update({
                "final_type": pattern_type,
                "decision_reason": "AI disabled"
            })
            self.explainability[column_name] = explain
            return pattern_type

        ai_result = self.ai_advisor.suggest_type(series, column_name)

        ai_type = ai_result.get("detected_type", "text")
        confidence = ai_result.get("confidence", 0)
        reason = ai_result.get("reason", "")

        explain.update({
            "ai_detected_type": ai_type,
            "ai_confidence": confidence,
            "ai_reason": reason
        })

        if confidence < self.threshold:
            final_type = pattern_type
            explain.update({
                "final_type": final_type,
                "decision_reason": "AI confidence below threshold",
                "ai_overruled": True
            })
        elif ai_type in ["date", "datetime"] and pattern_type == "numeric":
            final_type = "numeric"
            explain.update({
                "final_type": final_type,
                "decision_reason": "Pattern-based numeric override",
                "ai_overruled": True
            })
        else:
            final_type = ai_type
            explain.update({
                "final_type": final_type,
                "decision_reason": "AI accepted",
                "ai_overruled": False
            })

        self.explainability[column_name] = explain
        return final_type



    def clean_numeric(self, val):
        try:
            return float(str(val).replace(",", "").replace("$", ""))
        except:
            return val

    def standardize_email(self, val):
        return str(val).strip().lower()

    def standardize_phone(self, val):
        digits = re.sub(r"\D", "", str(val))
        return digits if len(digits) >= 7 else val

    def _standardize_text(self, series: pd.Series) -> pd.Series:
        return self.spelling_agent.correct_spelling_series(series)

    def _standardize_dates_ai(self, series: pd.Series) -> pd.Series:

        series = self.spelling_agent.correct_spelling_series(series)

        def parse(val):
            try:
                #return dateutil.parser.parse(str(val), fuzzy=True).strftime("%Y-%m-%d")
                return dateutil.parser.parse(str(val), fuzzy=True).strftime("%d-%m-%Y")
            except:
                return val

        return series.apply(parse)


    def _standardize_datetime_ai(self, series: pd.Series) -> pd.Series:

        series = self.spelling_agent.correct_spelling_series(series)

        def parse_datetime(val):
            try:
                parsed = dateutil.parser.parse(str(val), fuzzy=True)
                return parsed.strftime("%d-%m-%Y %H-%M-%S")
            except Exception as e:
                return val
        
        return series.apply(parse_datetime)
    

    def auto_standardize_column(self, series: pd.Series, column_name: str):

        pattern_type = self.detect_column_type_pattern(series)

        ai_result = None
        final_type = pattern_type
        reason = "Pattern-based detection"

        if self.use_ai:
            ai_result = self.ai_advisor.suggest_type(series, column_name)
            ai_type = ai_result.get("detected_type", "text")
            confidence = ai_result.get("confidence", 0)

            if confidence >= self.threshold:
                final_type = ai_type
                reason = "AI decision above confidence threshold"
            else:
                reason = "AI confidence below threshold, pattern used"

        
        self.explainability[column_name] = {
            "pattern_type": pattern_type,
            "ai_detected_type": ai_result.get("detected_type") if ai_result else None,
            "ai_confidence": ai_result.get("confidence") if ai_result else None,
            "ai_reason": ai_result.get("reason") if ai_result else None,
            "final_type": final_type,
            "decision_reason": reason
        }

        # Transformation
        if final_type == "date":
            return self._standardize_dates_ai(series)
        
        if final_type == "datetime":
            return self._standardize_datetime_ai(series)

        if final_type == "numeric":
            return series.apply(self.clean_numeric)

        if final_type == "email":
            return series.apply(self.standardize_email)

        if final_type == "phone":
            return series.apply(self.standardize_phone)


        col_type = self.detect_column_type(series, column_name)

        if col_type == "date":
            return self._standardize_dates_ai(series)
        if col_type == "datetime":
            return self._standardize_datetime_ai(series)
        if col_type == "numeric":
            return series.apply(self.clean_numeric)
        if col_type == "email":
            return series.apply(self.standardize_email)
        if col_type == "phone":
            return series.apply(self.standardize_phone)

        return self._standardize_text(series)

    def auto_standardize_dataframe(self, df: pd.DataFrame, columns=None):

        df_out = df.copy()
        cols = columns if columns else df.columns

        for col in cols:
            df_out[col] = self.auto_standardize_column(df[col], col)

        return df_out



    def get_explainability(self) -> Dict[str, Any]:
        return self.explainability


