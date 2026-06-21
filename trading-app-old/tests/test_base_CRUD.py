# import unittest
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from app.models import Base
# from app.services.models.Errors import DuplicateEntryError
#
#
# class TestCRUD(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls):
#         # Set up the in-memory database and session
#         engine = create_engine("sqlite:///:memory:")
#         Base.metadata.create_all(engine)
#         cls.Session = sessionmaker(bind=engine)
#
#     def setUp(self):
#         self.session = self.Session()
#         self.crud = CRUD(CurrentPosition, self.session)
#
#     def tearDown(self):
#         self.session.close()
#
#     def test_create_and_read(self):
#         # Create a new record
#         data = {"symbol": "AAPL", "quantity": 10, "average_price": 150.0}
#         self.crud.create(data)
#
#         # Read the record
#         result = self.crud.read({"symbol": "AAPL"})
#         self.assertEqual(len(result), 1)
#         self.assertEqual(result[0]["symbol"], "AAPL")
#         self.assertEqual(result[0]["quantity"], 10)
#         self.assertEqual(result[0]["average_price"], 150.0)
#
#     def test_update(self):
#         # Create a new record
#         data = {"symbol": "AAPL", "quantity": 10, "average_price": 150.0}
#         self.crud.create(data)
#
#         # Update the record
#         updated_data = {"symbol": "AAPL", "quantity": 15, "average_price": 155.0}
#         self.crud.update(updated_data)
#
#         # Read the updated record
#         result = self.crud.read({"symbol": "AAPL"})
#         self.assertEqual(result[0]["quantity"], 15)
#         self.assertEqual(result[0]["average_price"], 155.0)
#
#     def test_delete(self):
#         # Create a new record
#         data = {"symbol": "AAPL", "quantity": 10, "average_price": 150.0}
#         self.crud.create(data)
#
#         # Delete the record
#         self.crud.delete({"symbol": "AAPL"})
#
#         # Attempt to read the deleted record
#         result = self.crud.read({"symbol": "AAPL"})
#         self.assertEqual(len(result), 0)
#
#     def test_create_duplicate(self):
#         # Create a new record
#         data = {"symbol": "AAPL", "quantity": 10, "average_price": 150.0}
#         self.crud.create(data)
#
#         # Attempt to create a duplicate record
#         with self.assertRaises(DuplicateEntryError):
#             self.crud.create(data)
#
#
# if __name__ == "__main__":
#     unittest.main()
