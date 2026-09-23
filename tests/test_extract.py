import unittest

from src.extract import resolve_source_table


class SourceTableTests(unittest.TestCase):
    def test_resolves_fully_qualified_monthly_table(self):
        self.assertEqual(
            resolve_source_table(2026, 8),
            "hive_metastore.RH_DANONE.ceo_asistencia_08_2026",
        )

    def test_rejects_invalid_month(self):
        with self.assertRaisesRegex(ValueError, "target_month"):
            resolve_source_table(2026, 13)

    def test_rejects_invalid_year(self):
        with self.assertRaisesRegex(ValueError, "target_year"):
            resolve_source_table(26, 8)


if __name__ == "__main__":
    unittest.main()
