import pytest
from scdm_prepare.schema import TABLES


class TestSchemaDefinitions:
    def test_nine_tables_defined(self):
        """Verify exactly 9 tables are defined."""
        assert len(TABLES) == 9

    def test_all_tables_have_columns(self):
        """Verify each table has non-empty columns list."""
        for table in TABLES.values():
            assert table.columns, f"Table {table.name} has no columns"
            assert len(table.columns) > 0

    def test_all_tables_have_sort_keys(self):
        """Verify each table has non-empty sort_keys list."""
        for table in TABLES.values():
            assert table.sort_keys, f"Table {table.name} has no sort_keys"
            assert len(table.sort_keys) > 0

    def test_sort_keys_subset_of_columns(self):
        """Verify all sort_keys are a subset of columns for each table."""
        for table in TABLES.values():
            column_set = set(table.columns)
            sort_set = set(table.sort_keys)
            missing = sort_set - column_set
            assert not missing, (
                f"Table {table.name} has sort keys not in columns: {missing}"
            )

    def test_crosswalk_ids_keys_in_columns(self):
        """Verify all crosswalk_ids keys are present in the table's columns."""
        for table in TABLES.values():
            column_set = set(table.columns)
            crosswalk_set = set(table.crosswalk_ids.keys())
            missing = crosswalk_set - column_set
            assert not missing, (
                f"Table {table.name} has crosswalk IDs not in columns: {missing}"
            )

    def test_column_counts_match_documentation(self):
        """Verify column counts match tables_documentation.json."""
        expected_counts = {
            "enrollment": 8,
            "demographic": 9,
            "dispensing": 7,
            "encounter": 11,
            "diagnosis": 10,
            "procedure": 8,
            "facility": 2,
            "provider": 3,
            "death": 5,
        }
        for name, expected_count in expected_counts.items():
            table = TABLES[name]
            assert len(table.columns) == expected_count, (
                f"Table {name} has {len(table.columns)} columns, "
                f"expected {expected_count}"
            )

    def test_table_names_match_keys(self):
        """Verify each table's name matches its dictionary key."""
        for key, table in TABLES.items():
            assert table.name == key, (
                f"Table key '{key}' does not match table.name '{table.name}'"
            )

    def test_crosswalk_values_are_valid_join_types(self):
        """Verify crosswalk_ids values are valid join types."""
        valid_join_types = {"inner", "left"}
        for table in TABLES.values():
            for col_name, join_type in table.crosswalk_ids.items():
                assert join_type in valid_join_types, (
                    f"Table {table.name} column {col_name} has invalid join type "
                    f"'{join_type}'. Must be 'inner' or 'left'."
                )
