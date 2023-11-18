from pygments.style import Style
from pygments.token import Keyword, Name, Comment, String, Error, Number, Operator, Generic


class CustomStyle(Style):
    styles = {
        Keyword: 'bold #00FFD5',  # Cyan in hex format
        Name: 'italic #FFFF00',  # Yellow in hex format
        Comment: 'bold #FF00FF',  # Magenta in hex format
        String: '#FFFF00',  # Yellow in hex format
        Error: 'border:#FF0000',  # Red in hex format
        Number: 'bold #0000FF',  # Blue in hex format
        Operator: 'bold #FF0000',  # Red in hex format
        Generic: 'bold',  # Bold
    }

