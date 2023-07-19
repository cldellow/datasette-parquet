class DoubleQuoteForLiteraValue(Exception):
    """
    DuckDB follows the SQL standard more closely than SQLite,
    and as a result, is stricter about the use of double quotes
    to wrap literal values.
    Thrown when a literal value to compare against is wrapped in 
    double quotes instead of single quotes.

    For more info see the sqlite docs:
    https://www.sqlite.org/quirks.html#double_quoted_string_literals_are_accepted
    
    """
    def __init__(self, matches, message=None):
        if message:
            self.message = message
        else:
            self.message = (
                "It looks like you are using a double quoted string "
                f"for a value at: {matches[0]}. "
                "To make this work with DuckDB, wrap it in single quoted "
                "strings instead."
        )
        super().__init__(matches)
