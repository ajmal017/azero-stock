from ib_api import IBApp

app = IBApp("localhost", 4001, 20)
app.req_market_depth()