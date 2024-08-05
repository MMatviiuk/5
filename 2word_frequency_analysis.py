import string
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import requests
import matplotlib.pyplot as plt

def get_text(url):
    """Fetches text from the given URL.

    Args:
        url: The URL to fetch text from.

    Returns:
        The fetched text, or None if an error occurs.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for error HTTP status codes
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching the text: {e}")
        return None

def remove_punctuation(text):
    """Removes punctuation from the given text.

    Args:
        text: The input text.

    Returns:
        The text with punctuation removed.
    """
    return text.translate(str.maketrans('', '', string.punctuation))

def map_function(word):
    """Maps each word to a key-value pair (word, 1) for counting.

    Args:
        word: The individual word to be processed.

    Returns:
        A tuple containing the word and a count of 1 (word, 1).
    """
    return word, 1

def shuffle_function(mapped_values):
    """Shuffles the key-value pairs (word, count) for efficient reduction.

    Args:
        mapped_values: A list of tuples containing (word, count) pairs.

    Returns:
        An iterator of key-value pairs grouped by word, where each key (word)
        is associated with a list of its corresponding counts.
    """
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    """Reduces the shuffled word counts by summing the counts for each word.

    Args:
        key_values: An iterator where each element is a tuple (word, [count1, count2, ...]).

    Returns:
        A tuple containing the word and the summed count (word, total_count).
    """
    key, values = key_values
    return key, sum(values)

def map_reduce(text):
    """Performs MapReduce to analyze word frequencies in the given text.

    Args:
        text: The input text to analyze.

    Returns:
        A dictionary containing word frequencies, where keys are words and values are their counts.
    """
    # Preprocess text by removing punctuation and converting to lowercase
    text = remove_punctuation(text).lower()

    # Exclude common articles (optional)
    articles = {'the', 'a', 'an', 'of', 'to', 'and', 'in', 'it', 'that', 'not', 'as', 'is', 'at', 'for', 'but', 'on', 'or', 'by', 'from'}
    words = [word for word in text.split() if word not in articles]

    # Perform parallel mapping using a thread pool
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle key-value pairs for efficient reduction
    shuffled_values = shuffle_function(mapped_values)

    # Perform parallel reduction using a thread pool
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

def visualize_top_words(result, top_n=10):
    """Visualizes the top N most frequent words using a horizontal bar chart.

    Args:
        result: A dictionary containing word frequencies (word: count).
        top_n: The number of top words to visualize (default 10).
    """
    # Sort the word frequencies by count in descending order
    sorted_words = sorted(result.items(), key=lambda item: item[1], reverse=True)[:top_n]

    # Separate the words and their frequencies for plotting
    words, frequencies = zip(*sorted_words)

    # Create a horizontal bar chart
    plt.figure(figsize=(10, 8))
    plt.barh(words, frequencies, color='skyblue')
    plt.xlabel('Frequency')
    plt.ylabel('Words')
    plt.title(f'Top {top_n} Most Frequent Words')
    plt.gca().invert_yaxis()  # Invert y-axis to have the highest frequency word at the top
    plt.show()

if __name__ == "__main__":
    # Get the text from the URL
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = get_text(url)

    if text:
        # Perform MapReduce without filtering by search words
        result = map_reduce(text)

        # Visualize the top 10 words
        visualize_top_words(result, top_n=10)
    else:
        print("Error: Failed to fetch text.")
