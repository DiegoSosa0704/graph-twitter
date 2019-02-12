
from django.test import TestCase
from neo4j_database.database_insights import  Database_Insights

class TestDatabase(TestCase):

    def get_db(self):
        db = Database_Insights()
        return db


    def tes_comunities(self):
        db = self.get_db()
        result = db.search_comunity("category","is_in")
        assert result,"no lo hizo"

    def tes_central_node(self):
        db = self.get_db()
        result = db.search_central_node(1,"location")
        for tag in result:
            print(tag)

    def test_get_information(self):
        db = self.get_db()
        result = db.get_information_articles_for_comunity("category",13,"is_in","has","keyword")
        for tag in result:
            print(tag)