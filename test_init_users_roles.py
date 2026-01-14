import unittest

import init_users_roles as iur


class TestInitUsersRoles(unittest.TestCase):
    def test_admin_option_clause(self) -> None:
        self.assertEqual(iur.admin_option_clause("YES"), " WITH ADMIN OPTION")
        self.assertEqual(iur.admin_option_clause("Y"), " WITH ADMIN OPTION")
        self.assertEqual(iur.admin_option_clause("NO"), "")
        self.assertEqual(iur.admin_option_clause(None), "")

    def test_grant_satisfied(self) -> None:
        existing = {("A", "B"): {"NO"}}
        self.assertTrue(iur.grant_satisfied(existing, "A", "B", False))
        self.assertFalse(iur.grant_satisfied(existing, "A", "B", True))
        existing[("A", "B")].add("YES")
        self.assertTrue(iur.grant_satisfied(existing, "a", "b", True))

    def test_format_identifier(self) -> None:
        self.assertEqual(iur.format_identifier("TEST"), "TEST")
        self.assertEqual(iur.format_identifier("Test"), "\"Test\"")


if __name__ == "__main__":
    unittest.main()
