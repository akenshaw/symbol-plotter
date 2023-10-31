import sys
from xpv_dc_class import main_custom

if __name__ == "__main__":
    symbols = sys.argv[1:] 
    main_custom(symbols)