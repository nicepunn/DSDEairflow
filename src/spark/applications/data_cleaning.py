import re
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer, PorterStemmer

import contractions

def lower_case(text):
    """Converts all characters in the text to lowercase."""
    return text.lower()

def expand_contractions(text):
    expanded_text = []
    for word in text.split():
      expanded_text.append(contractions.fix(word))
    expanded_text = ' '.join(expanded_text)
    return expanded_text

def remove_usernames(text):
    """Removes usernames that start with '@'."""
    return re.sub(r'(@\w+)', ' ', text)

def isolate_and_remove_punctuations(text):
    """Isolates and selectively removes punctuations."""
    text = re.sub(r'([\'\"\.\(\)\!\?\\\/\,])', r' \1 ', text)
    text = re.sub(r'[^\w\s\?\%\/\.\-]', ' ', text)
    text = re.sub(r'\©.*?\d{4}\.', ' ', text)
    return text

def remove_numbers(text):
    """Removes numbers from the text."""
    return re.sub(r'\b\d+\.?\d*\b', ' ', text)

def remove_special_characters(text):
    """Removes special characters, preserving some for scientific relevance."""
    return re.sub(r'([\;\:\|•«\n])', ' ', text)

def remove_extra_whitespaces(text):
    """Removes extra whitespaces from the text."""
    return re.sub(r'\s+', ' ', text).strip()

def remove_emojis(text):
    """Removes emojis from the text."""
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def remove_repeated_punctuation(text):
    """Removes repeated punctuation marks."""
    return re.sub(r'([!?.]){2,}', r'\1', text)

def stem_words(text):
    """Applies stemming to each word in the text."""
    stemmer = PorterStemmer()
    token_words = word_tokenize(text)
    stem_sentence = []
    for word in token_words:
        stem_sentence.append(stemmer.stem(word))
        stem_sentence.append(" ")
    return "".join(stem_sentence)

def preserve_scientific_symbols(text):
    """Preserves scientific symbols and notations."""
    # Handling temperatures with spaces, e.g., "30 °C" or "100 °F"
    text = re.sub(r'(\d+)\s*°\s*C\b', r'\1 degrees Celsius', text)
    text = re.sub(r'(\d+)\s*°\s*F\b', r'\1 degrees Fahrenheit', text)

    # Area and Volume
    text = re.sub(r'\bm2/g\b', ' square meters per gram ', text)
    text = re.sub(r'\bcm3/g\b', ' cubic centimeters per gram ', text)

    # Additional notations as previously defined
    text = re.sub(r'\b°C\b', ' degrees Celsius ', text)
    text = re.sub(r'\b°F\b', ' degrees Fahrenheit ', text)
    text = re.sub(r'\bkm/s\b', ' kilometers per second ', text)
    text = re.sub(r'\bmg/L\b', ' milligrams per liter ', text)
    text = re.sub(r'\bPa\b', ' Pascals ', text)
    text = re.sub(r'\bkPa\b', ' kilopascals ', text)
    text = re.sub(r'\bMPa\b', ' megapascals ', text)
    text = re.sub(r'\bbar\b', ' bars ', text)
    text = re.sub(r'\bJ\b', ' Joules ', text)
    text = re.sub(r'\bkJ\b', ' kilojoules ', text)
    text = re.sub(r'\bMJ\b', ' megajoules ', text)
    text = re.sub(r'\bcal\b', ' calories ', text)
    text = re.sub(r'\bkcal\b', ' kilocalories ', text)
    text = re.sub(r'\bL/min\b', ' liters per minute ', text)
    text = re.sub(r'\bmL/s\b', ' milliliters per second ', text)
    text = re.sub(r'\bkg\b', ' kilograms ', text)
    text = re.sub(r'\bg\b', ' grams ', text)
    text = re.sub(r'\bmg\b', ' milligrams ', text)
    text = re.sub(r'\bkm\b', ' kilometers ', text)
    text = re.sub(r'\bm\b', ' meters ', text)
    text = re.sub(r'\bcm\b', ' centimeters ', text)
    text = re.sub(r'\bmm\b', ' millimeters ', text)

    return text


def text_preprocessing(s, remove_numbers_flag=False):
    s = preserve_scientific_symbols(s)  # Preserve scientific symbols before removing numbers
    s = lower_case(s)
    s = expand_contractions(s)
    s = remove_usernames(s)
    s = isolate_and_remove_punctuations(s)
    s = remove_emojis(s)
    s = remove_repeated_punctuation(s)
    if remove_numbers_flag: s = remove_numbers(s)
    s = remove_special_characters(s)
    s = remove_extra_whitespaces(s)
    return s