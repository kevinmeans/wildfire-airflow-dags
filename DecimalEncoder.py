import json
import decimal


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON Encoder for handling Decimal values

    This encoder will be used when converting Python objects to JSON. It will convert any
    Decimal values to a string representation.
    """

    def default(self, obj):
        """Handles encoding of Decimal values

        Args:
        obj (object): the object to be encoded.

        Returns:
        str: a string representation of the decimal value if obj is an instance of decimal.Decimal.
        object: the original obj otherwise.
        """

        # if passed in object is instance of Decimal
        # convert it to a string
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        # otherwise use the default behavior
        return json.JSONEncoder.default(self, obj)