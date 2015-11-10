import sys
import logging
import argparse
import psycopg2

# Set the log output file, and the log level
logging.basicConfig(filename = "snippets.log", level = logging.DEBUG)
# Connect to psql database
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect("dbname='snippets'")
logging.debug("Database connection established, get to work!")

def put(name, snippet):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute("insert into snippets values (%s, %s)", (name, snippet))
    except psycopg2.IntegrityError as e:
        with connection, connection.cursor() as cursor:
            cursor.execute("update snippets set message=%s where keyword=%s", (snippet, name))
            logging.debug("Duplicate snippet name used, existing snippet updated in place using an UPSERT.")
    logging.debug("Snippet stored successfully!")
    return name, snippet

def get(name):
    """Retrieve the snippet with a given name."""
    logging.info("Retrieving snippet {!r}".format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("select keyword, message from snippets where keyword=(%s)", (name,))
        row = cursor.fetchone()
    if not row:
        logging.info("Snippet '{}' not found and user notified".format(name))
        return print("Snippet '{}' not found, please try again!".format(name))
    logging.debug("Snippet message sucessfully retrieved!")
    return row[1]

def catalog():
    """Query for a list of keywords from the snippets table"""
    logging.info("Retrieving keywords from database")
    with connection, connection.cursor() as cursor:
        cursor.execute("select keyword from snippets order by keyword asc;")
        keywords = cursor.fetchall()
    logging.debug("Catalog of keywords sucessfully retrieved!")
    return keywords

def search(term):
    """Search is a means of listing snippets which contain a given string anywhere in their messages"""
    logging.info("Retrieving messages containing string: {!r}".format(term))
    with connection, connection.cursor() as cursor:
        cursor.execute("select * from snippets where message like '%{}%'".format(term,))
        results = cursor.fetchall()
    if not results:
        logging.info("Search string '{}' not found in any snippets and user notified".format(term))
        return print("Search string '{}' not found in any snippets, try another!".format(term))
    logging.debug("Messages containing search string sucessfully retrieved!")
    return results

def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description = "Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest = "command", help = "Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help = "Store a snippet")
    put_parser.add_argument("name", help = "The name of the snippet")
    put_parser.add_argument("snippet", help = "The snippet text")

    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help = "Get a stored snippet")
    get_parser.add_argument("name", help = "The name of the snippet")

    # Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    catalog_parser = subparsers.add_parser("catalog", help = "List a catalog of snippets stored")

    # Subparser for the search command
    logging.debug("Constructing search subparser")
    search_parser = subparsers.add_parser("search", help = "Search for string in a stored snippet")
    search_parser.add_argument("term", help = "The search term string")

    arguments = parser.parse_args(sys.argv[1:])

    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command == "catalog":
        keyword = catalog(**arguments)
        print("Retrieved catalog: {!r}".format(keyword))
    elif command == "search":
        term = search(**arguments)
        print("Snippets containing the search term: {!r}".format(term))

if __name__ == "__main__":
    main()
