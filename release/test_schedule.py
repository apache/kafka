#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for the schedule module.
Run with: cd release && python -m unittest test_schedule.py -v
"""

import argparse
import unittest
from datetime import date

import schedule


class TestSchedule(unittest.TestCase):

    def test_next_kip_freeze_slot(self):
        weeks = [9, 25, 41]
        self.assertEqual(date(2026, 10, 7), schedule.next_kip_freeze_slot(date(2026, 7, 27), weeks))
        self.assertEqual(date(2027, 3, 3), schedule.next_kip_freeze_slot(date(2027, 1, 13), weeks))
        # strictly after: a slot date itself moves on to the next slot
        self.assertEqual(date(2027, 6, 23), schedule.next_kip_freeze_slot(date(2027, 3, 3), weeks))

    def test_percentile(self):
        self.assertEqual(84, schedule.percentile([92, 84, 87, 63, 77], 50))
        self.assertEqual(87, schedule.percentile([92, 84, 87, 63, 77], 75))
        self.assertEqual(92, schedule.percentile([92, 84, 87, 63, 77], 100))

    def test_holiday_conflicts(self):
        self.assertTrue(schedule.holiday_conflicts("x", date(2026, 12, 9), date(2027, 1, 20)))
        self.assertTrue(schedule.holiday_conflicts("x", date(2027, 2, 3), date(2027, 2, 3)))
        self.assertFalse(schedule.holiday_conflicts("x", date(2027, 6, 1), date(2027, 7, 1)))

    def test_lunar_new_year(self):
        known = {2024: date(2024, 2, 10), 2027: date(2027, 2, 6), 2028: date(2028, 1, 26),
                 2029: date(2029, 2, 13), 2033: date(2033, 1, 31), 2034: date(2034, 2, 19),
                 2039: date(2039, 1, 24), 2040: date(2040, 2, 12)}
        for year, expected in known.items():
            self.assertEqual(expected, schedule.lunar_new_year(year), year)

    def test_parse_assignments(self):
        self.assertEqual({"4.4.0": date(2026, 10, 12)}, schedule.parse_assignments(["4.4.0=2026-10-12"]))
        with self.assertRaises(Exception):
            schedule.parse_assignments(["4.4.0"])

    def test_versions_until(self):
        self.assertEqual(["4.5.0", "4.6.0"], schedule.versions_until("4.4.0", "4.6.0"))
        self.assertIsNone(schedule.versions_until("4.4.0", "5.0.0"))

    def test_parse_scenarios(self):
        self.assertEqual([{}], schedule.parse_scenarios(None))
        self.assertEqual([{"4.5.0": date(2027, 1, 13)}], schedule.parse_scenarios(["4.5.0=2027-01-13"]))
        self.assertEqual([{"4.5.0": date(2027, 1, 13), "4.6.0": date(2027, 6, 2)},
                          {"4.5.0": date(2027, 1, 6), "4.6.0": date(2027, 6, 2)}],
                         schedule.parse_scenarios(["4.5.0=2027-01-13", "4.6.0=2027-06-02", "4.5.0=2027-01-06"]))

    def test_mail_date(self):
        self.assertEqual("Jan 6, 2027", schedule.mail_date(date(2027, 1, 6)))
        self.assertEqual("-", schedule.mail_date(None))

    def test_wednesday_on_or_before(self):
        self.assertEqual(date(2026, 11, 18), schedule.wednesday_on_or_before(date(2026, 11, 22)))
        self.assertEqual(date(2026, 11, 18), schedule.wednesday_on_or_before(date(2026, 11, 18)))

    def test_history_stats(self):
        history = [
            {"version": "4.2.0", "feature_freeze": date(2025, 11, 21), "first_rc": date(2026, 1, 13),
             "released": date(2026, 2, 16), "rc_count": 5},
            {"version": "4.3.0", "feature_freeze": date(2026, 3, 18), "first_rc": date(2026, 4, 28),
             "released": date(2026, 5, 20), "rc_count": 3},
            {"version": "4.4.0", "feature_freeze": date(2026, 7, 27), "first_rc": None,
             "released": None, "rc_count": 0},
        ]
        stats = schedule.history_stats(history, None)
        self.assertEqual([87, 63], stats["ff_to_release"])
        self.assertEqual([53, 41], stats["ff_to_rc0"])
        self.assertEqual([117, 131], stats["ff_interval"])
        self.assertEqual(["4.3.0", "4.4.0"], schedule.history_stats(history, 2)["versions"])

    def plan(self, count=1, overrides=None, iso_weeks=None, versions=None, ff_to_release=84, avoid_holidays=True):
        return schedule.build_plan("4.4.0", date(2026, 10, 12), date(2026, 7, 27), count, 3, 2, 4,
                                   139, 41, ff_to_release, overrides or {}, iso_weeks=iso_weeks,
                                   versions=versions, avoid_holidays=avoid_holidays)

    def test_plan_derived_from_history_moves_away_from_holidays(self):
        unshifted = self.plan(avoid_holidays=False)[0]
        self.assertEqual(date(2026, 11, 18), unshifted["kip_freeze"])
        self.assertEqual("history", unshifted["kip_freeze_source"])

        # unshifted code freeze would be 2026-12-23, moving one week earlier clears all holidays
        entry = self.plan()[0]
        self.assertEqual(date(2026, 11, 11), entry["kip_freeze"])
        self.assertEqual(date(2026, 12, 2), entry["feature_freeze"])
        self.assertEqual(date(2026, 12, 16), entry["code_freeze"])
        self.assertEqual(date(2027, 1, 12), entry["first_rc"])
        self.assertEqual(date(2027, 1, 13), entry["nominal"])
        self.assertEqual(date(2027, 2, 24), entry["realistic"])
        self.assertEqual("history, -1w for holidays", entry["kip_freeze_source"])

    def test_holiday_shift_never_before_previous_release(self):
        # previous release ships late, so the KIP freeze may only move later
        plan = schedule.build_plan("4.4.0", date(2026, 11, 15), date(2026, 7, 27), 1, 3, 2, 4,
                                   139, 41, 84, {})
        self.assertGreater(plan[0]["kip_freeze"], date(2026, 11, 15))
        self.assertIn("+", plan[0]["kip_freeze_source"])

    def test_plan_with_override_and_rule_flags_overlap(self):
        first, second = self.plan(count=2, overrides={"4.5.0": date(2027, 1, 13)}, iso_weeks=[9, 25, 41])
        self.assertEqual("given", first["kip_freeze_source"])
        self.assertEqual(date(2027, 4, 28), first["realistic"])
        self.assertEqual(date(2027, 3, 3), second["kip_freeze"])
        self.assertTrue(any("two release branches" in w for w in second["warnings"]))

    def test_realistic_never_before_nominal(self):
        entry = self.plan(ff_to_release=10)[0]
        self.assertEqual(entry["nominal"], entry["realistic"])

    def test_explicit_versions(self):
        plan = self.plan(count=2, versions=["4.9.0", "5.0.0"])
        self.assertEqual(["4.9.0", "5.0.0"], [p["version"] for p in plan])

    # Excerpts of the "Release Dates" section of real release plan pages, covering the formats in use
    PLAN_4_0 = ('<li><span>KIP Freeze: <strong><span style="color: rgb(34,34,34);">20th November 2024 </span>'
                '</strong>(Wednesday)</span></li><li><span>Feature Freeze: <strong>11th December 2024 </strong>'
                '(Wednesday)</span></li><li>Code Freeze: <strong>15th January 2025</strong> (Wednesday) At least '
                'two weeks of stabilization will follow Code Freeze, meaning we expect to release no earlier '
                'than <strong>January 29th 2025</strong>.</li>')
    PLAN_4_2 = ('KIP Freeze: October 29 (Wednesday) ... Feature Freeze: November 19 (Wednesday) ... '
                'Code Freeze: December 10 (Wednesday) ... we expect to release no earlier than January 7 2026 .')
    PLAN_4_4 = ('KIP Freeze: (8th July 2026 ) ... Feature Freeze: (29th July 2026) ... Code Freeze: '
                '(12th Aug 2026) ... we expect to release no earlier than September 9 2026 .')
    PLAN_4_5 = ('KIP Freeze: (January 13, 2027) ... Feature Freeze : (February 3, 2027) ... Feature Freeze: '
                '(February 3, 2027) ... Code Freeze: (February 17, 2027) ... release no earlier than '
                'March 17, 2027 .')

    def test_parse_release_plan(self):
        self.assertEqual({"kip_freeze": date(2024, 11, 20), "feature_freeze": date(2024, 12, 11),
                          "code_freeze": date(2025, 1, 15), "target": date(2025, 1, 29)},
                         schedule.parse_release_plan(self.PLAN_4_0, date(2024, 12, 16)))
        self.assertEqual({"kip_freeze": date(2026, 7, 8), "feature_freeze": date(2026, 7, 29),
                          "code_freeze": date(2026, 8, 12), "target": date(2026, 9, 9)},
                         schedule.parse_release_plan(self.PLAN_4_4, date(2026, 7, 27)))
        self.assertEqual(date(2027, 3, 17), schedule.parse_release_plan(self.PLAN_4_5, date(2027, 1, 1))["target"])

    def test_parse_release_plan_infers_missing_years(self):
        self.assertEqual({"kip_freeze": date(2025, 10, 29), "feature_freeze": date(2025, 11, 19),
                          "code_freeze": date(2025, 12, 10), "target": date(2026, 1, 7)},
                         schedule.parse_release_plan(self.PLAN_4_2, date(2025, 11, 21)))

    def test_parse_release_plan_not_following_template(self):
        self.assertIsNone(schedule.parse_release_plan("KIP Freeze: TBD", date(2026, 1, 1)))
        self.assertIsNone(schedule.parse_release_plan("<p>Some other page</p>", date(2026, 1, 1)))

    def test_published_plan_is_used_as_is(self):
        published = {"4.5.0": {"kip_freeze": date(2027, 1, 13), "feature_freeze": date(2027, 2, 3),
                               "code_freeze": date(2027, 2, 17), "target": date(2027, 3, 17)}}
        first, second = schedule.build_plan("4.4.0", date(2026, 10, 12), date(2026, 7, 27), 2, 3, 2, 4,
                                            131, 41, 84, {}, published=published)
        self.assertEqual("wiki release plan", first["kip_freeze_source"])
        self.assertEqual(date(2027, 3, 17), first["nominal"])
        self.assertEqual(date(2027, 4, 28), first["realistic"])
        # the next release is projected from the published feature freeze
        self.assertEqual("history", second["kip_freeze_source"])
        self.assertEqual(date(2027, 5, 19), second["kip_freeze"])

    def test_resolve_offsets_from_latest_plan(self):
        history = [{"version": "4.1.0", "planned": {"kip_freeze": date(2025, 5, 14), "feature_freeze": date(2025, 6, 4),
                                                   "code_freeze": date(2025, 6, 25), "target": date(2025, 7, 9)}},
                   {"version": "4.4.0", "planned": {"kip_freeze": date(2026, 7, 8), "feature_freeze": date(2026, 7, 29),
                                                   "code_freeze": date(2026, 8, 12), "target": date(2026, 9, 9)}},
                   {"version": "4.5.0", "planned": None}]
        args = argparse.Namespace(feature_freeze_weeks=None, code_freeze_weeks=5, target_weeks=None)
        schedule.resolve_offsets(args, history)
        self.assertEqual((3, 5, 4), (args.feature_freeze_weeks, args.code_freeze_weeks, args.target_weeks))
        self.assertEqual("the 4.4.0 release plan", args.offsets_source)

        args = argparse.Namespace(feature_freeze_weeks=None, code_freeze_weeks=None, target_weeks=None)
        schedule.resolve_offsets(args, [{"version": "4.4.0", "planned": None}])
        self.assertEqual((3, 2, 4), (args.feature_freeze_weeks, args.code_freeze_weeks, args.target_weeks))


if __name__ == "__main__":
    unittest.main()
