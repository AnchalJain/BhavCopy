import cherrypy
import config
from bhavcopy import BhavCopyDownLoader
from jinja2 import Environment, FileSystemLoader

jinja2_env = Environment(
    loader=FileSystemLoader([config.base_path])
)

bhav_copy = BhavCopyDownLoader()
bhav_copy.connect()
bhav_copy.insert_data()


class BSEBhavcopy(object):

    @cherrypy.expose
    def index(self):
        limit = 10
        template = jinja2_env.get_template("index.html")
        return template.render(data=bhav_copy.get_redis_data()[:limit])

    @cherrypy.expose
    def find(self, query):
        limit = 10
        template = jinja2_env.get_template("index.html")
        return template.render(data=bhav_copy.search_data_by_name(query)[:limit])


#  cherrypy.quickstart(HelloWorld())
if __name__ == '__main__':
    """
    Start CherryPy Engine
    """
    cherrypy.tree.mount(BSEBhavcopy(), "/", config.config)
    cherrypy.config.update({'server.socket_host': '0.0.0.0', 'server.socket_port': 80, })
    cherrypy.engine.start()
    cherrypy.engine.block()
