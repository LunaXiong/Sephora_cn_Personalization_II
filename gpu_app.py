import bottle
import json
import sys

app = bottle.app()


@app.route('/api/test')
def func():
    return json.dumps({"status": "ok", "msg": "success"})


port = sys.argv[1]

bottle.run(app, host='0.0.0.0', port=port)
