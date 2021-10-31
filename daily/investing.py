import investpy


data = investpy.get_etf_information(etf='SPDR S&P Global Natural Resources', country='united states', as_json=True)

print(data)