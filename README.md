# SEC
Using LLM's structured output to cheaply create data without licensing restrictions to enable research. This repo is for SEC data, which is easily downloadable and parseable via the [datamule](https://github.com/john-friedman/datamule-python) library.

Original Inspiration: a lunch at Tulsi Indian Eatery with Richard Li, Millenium Li, Kelly Ju, and Maggie Wang. [Article](https://medium.com/@jgfriedman99/how-to-create-alternative-datasets-using-datamule-d3a0192da8f6)

## Dependencies
Currently using the python package [txt2dataset](https://github.com/john-friedman/txt2dataset).

The package takes schema, text to structure, and an API KEY.

## Datasets
Datasets are stored in datasets/. They will be recompiled nightly using GH actions.

## Code to construct datasets
In scripts/

## Debug
Test scripts intended to be later productionized.

## How you can contribute:
1. Suggest [new datasets](https://github.com/Structured-Output/SEC/issues/1).
2. For suggested datasets, find where the information is recorded in text within the SEC corpus.
3. Check datasets for errors and bugs.
4. Test what schemas produce better outputs.

## API Credits

### Getting SEC Data
1. Go to the datamule website's [dashboard](https://datamule.xyz/dashboard2), and make a key.
2. Email [johnfriedman@datamule.xyz](mailto:johnfriedman@datamule.xyz), with subject [Structured Output], and I'll top you up.
3. Use [datamule-python](https://github.com/john-friedman/datamule-python) with the API Key to download data quickly.

### Getting LLM Credits
I'm planning to run the nightly GH actions off of $2k Google Cloud Credits. I can't share these credits. If people need credits, let me know, and I will see what I can do.

#### Google
Google has a $300 promotion, and provides decent access to Gemini on their free tier.



