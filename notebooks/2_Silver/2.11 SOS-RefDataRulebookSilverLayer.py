# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Databricks notebook source
# SOS ReferenceData — Rulebook v3

dbutils.widgets.text("catalog", "senseofsensors_catalog")
CATALOG       = dbutils.widgets.get("catalog").strip()
SILVER_SCHEMA = f"{CATALOG}.silver"
DATASET       = "ReferenceData"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
print("Silver schema ready:", SILVER_SCHEMA)

# Date Format(s)
# 4-digit year variants first, then 2-digit. Silver always writes yyyy-MM-dd.
_DATE_FMTS = (
    "d/M/yyyy|dd/MM/yyyy|d/MM/yyyy|dd/M/yyyy"
    "|d-M-yyyy|dd-MM-yyyy|d-MM-yyyy|dd-M-yyyy"
    "|yyyy-MM-dd|yyyy/MM/dd|yyyyMMdd"
    "|d MMM yyyy|dd MMM yyyy|d-MMM-yyyy|dd-MMM-yyyy"
    "|d/M/yy|dd/MM/yy|d/MM/yy|dd/M/yy"
    "|d-M-yy|dd-MM-yy"
)

# Column rulebook
# Tuple layout (17 values):
#   0  SourceColumn      — bronze column name; None for derived columns
#   1  TargetColumn      — silver PascalCase name
#   2  DataType          — Spark type string
#   3  IsRequired        — bool
#   4  MinValue          — numeric lower bound or None
#   5  MaxValue          — numeric upper bound or None
#   6  CanonicalUnit
#   7  AllowedUnits
#   8  SemanticGroup
#   9  Description
#  10  CleaningStrategy  — semicolon-separated tokens
#  11  KeepRaw           — always False
#  12  AcceptedFormats
#  13  InvalidAction
#  14  WarningType
#  15  Pattern
#  16  AllowedValues

reference_rules = [

    # Identity
    # Farm: 8 known numeric farm IDs (e.g. 1234). Stored as string.
    ('Farm_0', 'HerdIdentifier', 'string', True, None, None, None, None,
     'Identity',
     'Farm / herd identifier. All known values are numeric strings (e.g. 1234). '
     'Stored as string to preserve leading zeros.',
     'Trim;RemoveTrailingDotZero;NormalizeToString',
     False, None, 'Warn', 'InvalidHerdIdentifierFormat', None, None),

    # Animal: numeric string identifiers.
    ('Animal_1', 'AnimalIdentifier', 'string', True, None, None, None, None,
     'Identity', 'Animal number within the farm. Numeric string.',
     'Trim;NormalizeToString',
     False, None, 'Warn', 'InvalidAnimalIdentifierFormat', None, None),

    # LifeNumber: mostly NL prefix, one DK found. Leading space stripped.
    # Asterisk suffix (*) on duplicates stripped; asterisk rows are flagged by dedup step.
    ('Lifenumber_2', 'LifeNumber', 'string', True, None, None, None, None,
     'Identity',
     "National life number. Mostly NL prefix (e.g. NL 123456789)"
     "Normalised to single-spaced upper-case.",
     'Trim;UpperCase;CollapseSpaces;StripSuffix(*);LifeNumberFormat',
     False, None, 'Warn', 'InvalidLifeNumberFormat', r'^[A-Z]{2}\s\d+$', None),

    # Parity: observed range 1-13. 0 (heifer) retained as biologically valid.
    ('Parity_3', 'Parity', 'int', False, 0, 13, None, None,
     'Identity',
     'Lactation number. 0 = heifer. Observed range in data: 1-13. '
     'Text like "2 en 3" -> NULL + InvalidNumeric warning.',
     'NumericExtract;IntCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # DryPeriod
    ('ExpectedCalvingDate_4', 'ExpectedCalvingDate', 'date', False, None, None, None, None,
     'DryPeriod', 'Expected calving date.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    ('DateScoreStartDryPeriod_5', 'DryStartScoreDate', 'date', False, None, None, None, None,
     'DryPeriod', 'Date of body condition scoring at dry-off.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # BcsStartDryPeriod: observed 1-5. Values 6,7 exist but are implausible on 1-5 scale
    # -> capped at 5.0 + OutOfExpectedRange warning (cap not null — preserves the data intent).
    # "2 5" (space typo) -> "2.5" via SpaceToDecimal. "3/01/00" is a date-format typo -> NULL.
    # "/" not valid as decimal -> NULL + InvalidNumeric warning (not conversion).
    ('BCSStart_6', 'BcsStartDryPeriod', 'double', False, 1, 5, 'Score1To5', 'Score1To5',
     'DryPeriod',
     'Body condition score at dry-off (1-5 scale). '
     'Space typo "2 5" converted to 2.5. '
     'Slash-format date typos (e.g. 3/01/00) -> NULL + InvalidNumeric warning. '
     'Values above 5 capped at 5.0 + OutOfExpectedRange warning.',
     'Trim;SpaceToDecimal;CommaToDot;RejectSlashDate;DoubleCast;CapMax(5)',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # LocoStartDryPeriod: clean 1-5 plus one "2,75" -> 2.75.
    ('LOCOStart_7', 'LocoStartDryPeriod', 'double', False, 1, 5, 'Score1To5', 'Score1To5',
     'DryPeriod',
     'Locomotion score at dry-off (1-5 scale). Comma decimal "2,75" normalised to 2.75.',
     'CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange', None, None),

    ('DateScoreEndDryPeriod_8', 'DryEndScoreDate', 'date', False, None, None, None, None,
     'DryPeriod', 'Date of body condition scoring at end of dry period.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # BcsEndDryPeriod: "3,25" -> 3.25. Range 1-5.
    ('BCSEnd_9', 'BcsEndDryPeriod', 'double', False, 1, 5, 'Score1To5', 'Score1To5',
     'DryPeriod',
     'Body condition score at end of dry period (1-5 scale). Comma decimal normalised.',
     'CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange', None, None),

    # LocoEndDryPeriod: "2/01/1900" is a date-format typo -> NULL + InvalidNumeric warning.
    ('LOCOEnd_10', 'LocoEndDryPeriod', 'double', False, 1, 5, 'Score1To5', 'Score1To5',
     'DryPeriod',
     'Locomotion score at end of dry period (1-5 scale). '
     'Slash-format date typo "2/01/1900" -> NULL + InvalidNumeric warning.',
     'Trim;RejectSlashDate;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # Calving
    ('CalvingDate_11', 'CalvingDate', 'date', False, None, None, None, None,
     'Calving', 'Actual calving date.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # CalvingTime: canonical HH:mm:ss (24-hour).
    # Step 1: TypoFix -> replace ! with 1 (e.g. 14:!0 -> 14:10).
    # Step 2: values prefixed with +/- kept raw + ApproximateTime warning.
    # Step 3: AM/PM converted to 24-hour.
    # Step 4: unparseable after all fixes -> NULL + InvalidTimeFormat warning.
    ('CalvingTime_12', 'CalvingTime', 'string', False, None, None, None, None,
     'Calving',
     'Calving time of day; normalised to HH:mm:ss (24-hour). '
     'Typo fix: ! -> 1 before parsing. '
     '+/- prefix values kept raw + ApproximateTime warning. '
     'AM/PM converted. Unparseable values -> NULL + InvalidTimeFormat warning.',
     'Trim;TypoFix(!->1);ApproximateTimeFlag;NormalizeTime24h',
     False, None, 'Warn', 'InvalidTimeFormat|ApproximateTime', None, None),

    # CalvingType: codes 1-5 confirmed. Code 6 observed but not in codebook -> overflow.
    # "2 en 3" means the animal had characteristics of both codes 2 and 3
    # -> stored as "2;3" in ClinicalObservationsOverflow with source prefix,
    #    CalvingType set to NULL + MultiCodeValue warning.
    # "2,1" is ambiguous (typo? code?) -> overflow + NULL.
    ('CalvingType_13', 'CalvingType', 'string', False, None, None, 'Code', 'Code',
     'Calving',
     'Calving ease code (1=Normal, 2=Assisted, 3=Difficult, 4=Caesarean, 5=Stillbirth). '
     'Multi-code values (e.g. "2 en 3") -> stored as "CalvingType:2;3" in ClinicalObservationsOverflow, '
     'CalvingType set to NULL + MultiCodeValue warning. '
     'Code 6 and ambiguous values (e.g. "2,1") -> overflow + NULL + UnknownCode warning.',
     'Trim;UpperCase;MultiCodeToOverflow(en);CodeMap',
     False, None, 'Warn', 'UnknownCode|MultiCodeValue', None, '1|2|3|4|5'),

    # CalvingSignal: codes 1-6. Blank space -> NULL
    ('CalvingSignal_14', 'CalvingSignal', 'string', False, None, None, 'Code', 'Code',
     'Calving',
     'Calving signal / detection method code '
     '(1=NoSignal, 2=TailUp, 3=SensorAlert, 4=VisualCheck, 5=FarmerObservation, 6=Other). '
     'PROVISIONAL labels — verify against farm system codebook. '
     'Values outside 1-6 -> NULL + UnknownCode warning.',
     'Trim;UpperCase;CodeMap',
     False, None, 'Warn', 'UnknownCode', None, '1|2|3|4|5|6'),

    # CalvingDurationHr: decimal hours. Observed range 0.07-21.5 (no values above 24).
    # Values like 10.15, 14.25 may be clock times (10:15, 14:25) entered as decimals;
    # flagged with SuspectedClockTime warning when value has .15/.25/.30/.45 pattern
    # AND is above 6 (unlikely calving > 6h). Not nulled — preserved with warning.
    # Comma decimal normalised. Renamed from CalvingDurationHours for brevity.
    ('CalvingDuration_15', 'CalvingDurationHr', 'double', False, 0, 24, 'Hours', 'Hours',
     'Calving',
     'Duration of calving in decimal hours (e.g. 1.5 = 90 min). '
     'Comma decimal normalised. Observed range: 0.07-21.5 h. '
     'Values with clock-time patterns (e.g. 10.15, 14.25) above 6 h receive '
     'SuspectedClockTime warning — kept not nulled.',
     'Trim;CommaToDot;DoubleCast;FlagSuspectedClockTime',
     False, None, 'Warn', 'OutOfExpectedRange|SuspectedClockTime', None, None),

    # CalfRemoval: codes 1-6. All clean in data.
    ('CalfRemoval_16', 'CalfRemoval', 'string', False, None, None, 'Code', 'Code',
     'Calving',
     'Calf removal timing code '
     '(1=Immediate, 2=AfterColostrum, 3=After1Day, 4=AfterSeveralDays, 5=CalfDied, 6=Other). '
     'PROVISIONAL labels — verify against farm system codebook.',
     'Trim;UpperCase;CodeMap',
     False, None, 'Warn', 'UnknownCode', None, '1|2|3|4|5|6'),

    # Afterbirth: codes 1-4. All clean in data.
    ('Afterbirth_17', 'Afterbirth', 'string', False, None, None, 'Code', 'Code',
     'Calving',
     'Placenta / afterbirth outcome code '
     '(1=NormalShedWithin8h, 2=RetainedShedNaturally, 3=RetainedManuallyRemoved, 4=RetainedNotRemoved).',
     'Trim;UpperCase;CodeMap',
     False, None, 'Warn', 'UnknownCode', None, '1|2|3|4'),

    # Notes: free-text Dutch.
    ('Notes_18', 'Notes', 'string', False, None, None, None, None,
     'Calving',
     'Free-text clinical notes in Dutch. Trimmed and whitespace-normalised only in silver. '
     'TranslationGlossary-based condition flags derived in gold.',
     'Trim;NormalizeSpaces',
     False, None, None, None, None, None),

    # Calcium
    ('CalciumDate_19', 'CalciumDate', 'date', False, None, None, None, None,
     'Calcium', 'Blood calcium sample date.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # DimCalcium: observed valid range 0-6. Excel-serial negatives (-42760, -42765) -> NULL.
    # Biologically calcium is drawn 0-14 DIM so warn above 14.
    ('DIMCalcium_20', 'DimCalcium', 'int', False, 0, 14, 'Days', 'Days',
     'Calcium',
     'Days in milk at calcium sample. Observed values: 0-6. '
     'Biologically plausible upper bound set to 14 DIM. '
     'Excel-serial negative values (-42760, -42765) -> NULL + InvalidNumeric warning.',
     'Trim;RejectNegativeExcelSerial;NumericExtract;IntCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # CalciumLevel: observed 0.99-3.59 mmol/L. Biologically 0-4 is valid.
    ('CalciumLevel_21', 'CalciumLevel', 'double', False, 0, 4, 'MmolPerL', 'MmolPerL|MgPerDL',
     'Calcium',
     'Blood calcium in mmol/L. Observed range: 0.99-3.59. '
     'Values below ~2.0 indicate subclinical hypocalcaemia. '
     'MgPerDL input converted via UnitConversionLookup (/ 4.008).',
     'CommaToDot;DoubleCast;UnitConvert',
     False, None, 'Warn', 'OutOfExpectedRange|UnknownUnit', None, None),

    # Ketosis (BHBA)
    ('BHBA1Date_22', 'BHBA1Date', 'date', False, None, None, None, None,
     'Ketosis', 'First BHBA sample date.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # DimBhba1: observed 0-12. One date value "21/03/2017" -> NULL + InvalidNumeric warning.
    # Upper bound 14 to match calcium (same early lactation window).
    ('DIMBHBA1_23', 'DimBhba1', 'int', False, 0, 14, 'Days', 'Days',
     'Ketosis',
     'Days in milk at first BHBA sample. Observed range: 0-12. '
     'Date value "21/03/2017" found in data -> NULL + InvalidNumeric warning.',
     'Trim;RejectSlashDate;NumericExtract;IntCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # BHBA1: observed 0.2-5.1 mmol/L. Range 0-6 allows for severe cases.
    ('BHBA1_24', 'BHBA1', 'double', False, 0, 6, 'MmolPerL', 'MmolPerL|MgPerDL',
     'Ketosis',
     'First BHBA mmol/L. Observed range: 0.2-5.1. '
     'Values >= 1.2 indicate ketosis. Range 0-6 allows for severe cases. '
     'MgPerDL input converted via UnitConversionLookup (* 0.0961).',
     'CommaToDot;DoubleCast;UnitConvert',
     False, None, 'Warn', 'OutOfExpectedRange|UnknownUnit', None, None),

    ('BHBA2Date_25', 'BHBA2Date', 'date', False, None, None, None, None,
     'Ketosis', 'Second BHBA sample date.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # DimBhba2: observed 7-25. Two garbage values: -353, -43199 (Excel serials) -> NULL.
    # Date value "28/03/2017" -> NULL. Upper bound 30 DIM covers retest window.
    ('DIMBHBA2_26', 'DimBhba2', 'int', False, 0, 30, 'Days', 'Days',
     'Ketosis',
     'Days in milk at second BHBA sample. Observed range: 7-25. '
     'Excel-serial values (-353, -43199) and date value "28/03/2017" -> NULL + InvalidNumeric warning.',
     'Trim;RejectNegativeExcelSerial;RejectSlashDate;NumericExtract;IntCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # BHBA2: observed 0.2-10.0. Comma decimals (0,4 etc) -> dot. Range 0-10.
    ('BHBA2_27', 'BHBA2', 'double', False, 0, 10, 'MmolPerL', 'MmolPerL|MgPerDL',
     'Ketosis',
     'Second BHBA mmol/L. Observed range: 0.2-10.0. '
     'Comma decimals normalised (e.g. 0,4 -> 0.4). '
     'MgPerDL input converted via UnitConversionLookup (* 0.0961).',
     'CommaToDot;DoubleCast;UnitConvert',
     False, None, 'Warn', 'OutOfExpectedRange|UnknownUnit', None, None),

    # ReproWeek4
    ('RE4Date_28', 'ReproExam4Date', 'date', False, None, None, None, None,
     'ReproWeek4', 'Reproduction examination date at week 4 post-calving.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # U4: codes S,A,I,V. Lowercase v -> V. "?" -> NULL + UnknownCode warning.
    # Date value "11/04/2017" -> NULL + InvalidCode warning.
    ('U4_29', 'Repro4Uterus', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek4',
     'Uterus finding code at week 4 (S=Normal, A=Abnormal, I=Involuting, V=Other). '
     'Lowercase normalised to uppercase. '
     '"?" and date values -> NULL + InvalidCode warning.',
     'Trim;UpperCase;RejectSlashDate;CodeMap',
     False, None, 'Warn', 'UnknownCode|InvalidCode', None, 'S|A|I|V'),

    # LO4: codes A,C,I,S. Lowercase a -> A. No dirty values beyond case.
    ('LO4_30', 'Repro4LeftOvary', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek4',
     'Left ovary finding code at week 4. Observed values: A, C, I, S. '
     'Lowercase normalised to uppercase.',
     'Trim;UpperCase',
     False, None, 'Warn', 'UnknownCode', None, 'A|C|I|S'),

    # RO4: codes A,C,I,S. Lowercase a,c -> uppercase.
    ('RO4_31', 'Repro4RightOvary', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek4',
     'Right ovary finding code at week 4. Observed values: A, C, I, S. '
     'Lowercase normalised to uppercase.',
     'Trim;UpperCase',
     False, None, 'Warn', 'UnknownCode', None, 'A|C|I|S'),

    # TON4: allowed +, -, +/-, -/+.
    # Typo fixes before validation:
    #   Backtick prefix (`+) -> +
    #   ++ -> +   (duplicate positive)
    #   -- -> -   (duplicate negative; note space between may appear)
    #   Excess whitespace stripped.
    # "A" -> ClinicalObservationsOverflow + NULL + UnknownCode warning.
    ('TON4_32', 'Repro4UterineTone', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek4',
     'Uterine tone at week 4. Allowed values: +, -, +/-, -/+. '
     'Typo fixes: backtick stripped, ++ -> +, -- -> -. '
     '"A" -> ClinicalObservationsOverflow + NULL + UnknownCode warning.',
     'Trim;UterineToneTypoFix;MoveUnknownToOverflow',
     False, None, 'Warn', 'UnknownCode', None, '+|-|+/-|-/+'),

    # INV4: allowed +, -, +/-.
    # Underscore "_" -> "-" (farmer shorthand for negative/absent).
    # Typo fixes: ++ -> +, -- -> -.
    ('INV4_33', 'Repro4Involution', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek4',
     'Uterine involution at week 4. Allowed values: +, -, +/-. '
     'Underscore "_" converted to "-". Typo fixes: ++ -> +, -- -> -.',
     'Trim;UnderscoreToMinus;UterineToneTypoFix',
     False, None, 'Warn', 'UnknownCode', None, '+|-|+/-'),

    # BCS4: range 1-5. Values below 1 (0.013, 0.12, 0.4) are implausible
    # -> NULL + InvalidNumeric warning. "+" -> NULL + InvalidNumeric warning.
    # Comma decimal normalised.
    ('BCS4_34', 'BcsWeek4', 'double', False, 1, 5, 'Score1To5', 'Score1To5',
     'ReproWeek4',
     'Body condition score at week 4 (1-5 scale). '
     'Comma decimal normalised. '
     'Values below 1 (e.g. 0.013, 0.12, 0.4) are implausible -> NULL + InvalidNumeric warning. '
     '"+" -> NULL + InvalidNumeric warning.',
     'Trim;RejectNonNumeric;CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # LOCO4: range 1-5. "20" is a typo for "2" (extra zero) -> 2.
    # "3,00" -> 3.0. "-" -> NULL via CF02.
    ('LOCO4_35', 'LocoWeek4', 'double', False, 1, 5, 'Score1To5', 'Score1To5',
     'ReproWeek4',
     'Locomotion score at week 4 (1-5 scale). '
     '"20" corrected to 2 (extra-zero typo). Comma decimal normalised. '
     '"- " -> NULL via CF02.',
     'Trim;ExtraZeroFix(20->2);CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # CycleWeek4: clean integers 0-21. Lone dash/space -> NULL
    ('Cycle4_36', 'CycleWeek4', 'int', False, 0, 25, 'Days', 'Days',
     'ReproWeek4',
     'Cycle day at week 4 reproductive examination. Observed range: 0-21. '
     'Lone dash or space -> NULL (not recorded) via CF02.',
     'NumericExtract;IntCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # ReproWeek8
    ('RE8Date_37', 'ReproExam8Date', 'date', False, None, None, None, None,
     'ReproWeek8', 'Reproduction examination date at week 8 post-calving.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # U8: codes S,V only observed (plus ? and a date). Same rules as U4.
    ('U8_38', 'Repro8Uterus', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek8',
     'Uterus finding code at week 8 (S=Normal, A=Abnormal, I=Involuting, V=Other). '
     '"?" and date values -> NULL + InvalidCode warning.',
     'Trim;UpperCase;RejectSlashDate;CodeMap',
     False, None, 'Warn', 'UnknownCode|InvalidCode', None, 'S|A|I|V'),

    # LO8: codes A,C,I,S. "Tumor" is a clinical finding (not a code)
    # -> moved to ClinicalObservationsOverflow prefixed with "Repro8LeftOvary:".
    ('LO8_39', 'Repro8LeftOvary', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek8',
     'Left ovary finding code at week 8. Observed values: A, C, I, S. '
     '"Tumor" moved to ClinicalObservationsOverflow with prefix "Repro8LeftOvary:" + UnknownCode warning.',
     'Trim;UpperCase;MoveUnknownToOverflow',
     False, None, 'Warn', 'UnknownCode', None, 'A|C|I|S'),

    # RO8: codes A,C,I,S. Leading/trailing spaces stripped. Duplicate spaces in " A" stripped.
    ('RO8_40', 'Repro8RightOvary', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek8',
     'Right ovary finding code at week 8. Observed values: A, C, I, S. '
     'Whitespace stripped.',
     'Trim;UpperCase',
     False, None, 'Warn', 'UnknownCode', None, 'A|C|I|S'),

    # TON8: same typo rules as TON4. "A" -> overflow.
    ('TON8_41', 'Repro8UterineTone', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek8',
     'Uterine tone at week 8. Allowed values: +, -, +/-, -/+. '
     'Typo fixes: ++ -> +, -- -> -, backtick stripped. '
     '"A" -> ClinicalObservationsOverflow + NULL.',
     'Trim;UterineToneTypoFix;MoveUnknownToOverflow',
     False, None, 'Warn', 'UnknownCode', None, '+|-|+/-|-/+'),

    # INV8: same as INV4. No underscore observed but rule kept for consistency.
    ('INV8_42', 'Repro8Involution', 'string', False, None, None, 'Code', 'Code',
     'ReproWeek8',
     'Uterine involution at week 8. Allowed values: +, -, +/-. '
     'Typo fixes: ++ -> +, -- -> -.',
     'Trim;UterineToneTypoFix',
     False, None, 'Warn', 'UnknownCode', None, '+|-|+/-'),

    # BCS8: range 1-4 observed (max 4.25). "2m5" -> 2.5 (m is decimal typo).
    # "+" and "-" -> NULL + InvalidNumeric warning. Comma decimal normalised.
    ('BCS8_43', 'BcsWeek8', 'double', False, 1, 5, 'Score1To5', 'Score1To5',
     'ReproWeek8',
     'Body condition score at week 8 (1-5 scale). Observed max: 4.25. '
     '"2m5" corrected to 2.5 (m-as-decimal typo). Comma decimal normalised. '
     '"+" and "-" -> NULL + InvalidNumeric warning.',
     'Trim;MTypoFix(m->dot);RejectNonNumeric;CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # LOCO8: clean 1-5. No dirty values.
    ('LOCO8_44', 'LocoWeek8', 'double', False, 1, 5, 'Score1To5', 'Score1To5',
     'ReproWeek8', 'Locomotion score at week 8 (1-5 scale).',
     'CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange', None, None),

    # CycleWeek8: numeric part extracted. Pregnancy text -> CycleWeek8EarlyPregnancySuspected.
    # Raw text preserved in CycleWeek8RawText.
        ('Cycle8_45', 'CycleWeek8', 'int', False, 0, 25, 'Days', 'Days',
     'ReproWeek8',
     'Cycle day at week 8 reproductive examination. Observed numeric range: 0-21. '
     'Asterisk suffix (*) stripped. '
     'Non-numeric text (e.g. "Mogelijk vroege dracht", "9/Vroege dracht") moved to '
     'ClinicalObservationsOverflow prefixed "CycleWeek8:".',
     'NumericExtract;IntCast;StripSuffix(*);MoveNonNumericToOverflow',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # IndividualAttention 1-4: free-text Dutch.
    # AttentionType columns: numeric codes 1-9 expected.
    # Non-numeric values moved to ClinicalObservationsOverflow (one shared column)
    # prefixed with source column name, semicolon-separated when multiple sources contribute.
    ('IndividueleAttentie_46', 'IndividualAttention1', 'string', False, None, None, None, None,
     'Attention',
     'Individual attention / alert note 1 (free-text Dutch). '
     'Trimmed and whitespace-normalised in silver. Flagging via TranslationGlossary in gold.',
     'Trim;NormalizeSpaces',
     False, None, None, None, None, None),

    ('DatumIndividueleAttentie_47', 'IndividualAttention1Date', 'date', False, None, None, None, None,
     'Attention', 'Date of individual attention / alert 1.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    ('TypeAttentie_48', 'IndividualAttention1Type', 'string', False, None, None, 'Code', 'Code',
     'Attention',
     'Attention type code 1. Farm system codes 1-9. '
     'Non-numeric values moved to ClinicalObservationsOverflow prefixed "IndividualAttention1Type:" '
     '+ InvalidAttentionType warning.',
     'Trim;MoveNonNumericToOverflow',
     False, None, 'Warn', 'InvalidAttentionType', None, '1|2|3|4|5|6|7|8|9'),

    ('IndividueleAttentie2_49', 'IndividualAttention2', 'string', False, None, None, None, None,
     'Attention',
     'Individual attention / alert note 2 (free-text Dutch). '
     'Trimmed and whitespace-normalised in silver.',
     'Trim;NormalizeSpaces',
     False, None, None, None, None, None),

    ('DatumIndividueleAttentie2_50', 'IndividualAttention2Date', 'date', False, None, None, None, None,
     'Attention', 'Date of individual attention / alert 2.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    ('TypeAttentie11_51', 'IndividualAttention2Type', 'string', False, None, None, 'Code', 'Code',
     'Attention',
     'Attention type code 2. Farm system codes 1-9. '
     'Non-numeric values (e.g. "mastitis", "?") moved to ClinicalObservationsOverflow '
     'prefixed "IndividualAttention2Type:" + InvalidAttentionType warning.',
     'Trim;MoveNonNumericToOverflow',
     False, None, 'Warn', 'InvalidAttentionType', None, '1|2|3|4|5|6|7|8|9'),

    ('IndividueleAttentie3_52', 'IndividualAttention3', 'string', False, None, None, None, None,
     'Attention',
     'Individual attention / alert note 3 (free-text Dutch). '
     'Trimmed and whitespace-normalised in silver.',
     'Trim;NormalizeSpaces',
     False, None, None, None, None, None),

    ('DatumIndividueleAttentie3_53', 'IndividualAttention3Date', 'date', False, None, None, None, None,
     'Attention', 'Date of individual attention / alert 3.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    ('TypeAttentie12_54', 'IndividualAttention3Type', 'string', False, None, None, 'Code', 'Code',
     'Attention',
     'Attention type code 3. Farm system codes 1-9. All values in data are clean codes.',
     'Trim;MoveNonNumericToOverflow',
     False, None, 'Warn', 'InvalidAttentionType', None, '1|2|3|4|5|6|7|8|9'),

    ('IndividueleAttentie4_55', 'IndividualAttention4', 'string', False, None, None, None, None,
     'Attention',
     'Individual attention / alert note 4 (free-text Dutch). '
     'Trimmed and whitespace-normalised in silver. '
     'Includes values like "0" and "1" alongside free text — all preserved.',
     'Trim;NormalizeSpaces',
     False, None, None, None, None, None),

    ('DatumIndividueleAttentie4_56', 'IndividualAttention4Date', 'date', False, None, None, None, None,
     'Attention', 'Date of individual attention / alert 4.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # TypeAttentie13_57: mostly Excel serial dates (42xxx/43xxx) with a few real codes.
    # Excel serials -> NULL + InvalidAttentionType warning.
    # "1 en 7" -> overflow.
    # Real codes 0,3,4,5,7,9 -> kept (0 included as some systems use 0-based).
    ('TypeAttentie13_57', 'IndividualAttention4Type', 'string', False, None, None, 'Code', 'Code',
     'Attention',
     'Attention type code 4. Farm system codes 1-9. '
     'Majority of values are Excel serial dates (42xxx-43xxx) entered in error '
     '-> NULL + InvalidAttentionType warning. '
     'Multi-code strings (e.g. "1 en 7") and other non-numerics '
     '-> ClinicalObservationsOverflow prefixed "IndividualAttention4Type:".',
     'Trim;RejectExcelSerial;MoveNonNumericToOverflow',
     False, None, 'Warn', 'InvalidAttentionType', None, '1|2|3|4|5|6|7|8|9'),

    # ClinicalObservationsOverflow: ONE shared derived column.
    # Collects all non-code values from code columns and unusual findings.
    # Format: "SourceColumnName:value; SourceColumnName:value; ..."
    # Sources that contribute: CalvingType (multi-codes), TON4/8 (A),
    #   LO8 (Tumor), IndividualAttentionNType (free text),
    #   KetosisAttention1MPR (free text), TweedeKetoseAttentieDIM (free text).
    (None, 'ClinicalObservationsOverflow', 'string', False, None, None, None, None,
     'Overflow',
     'Collects non-code and unusual text values from any column where the value '
     'cannot be stored in its typed source column. '
     'Format: "SourceColumn:value; SourceColumn:value". '
     'Applies to all columns except Notes and IndividualAttention1-4 which are '
     'already free-text string columns. '
     'NULL when no overflow values exist for the row.',
     'DerivedOverflow',
     False, None, None, None, None, None),

    # MPR
    # KetosisAttention1MPR: integer DIM values 0-54. Two free-text values -> overflow.
    ('KetoseAttentieMPR_58', 'KetosisAttention1MPR', 'int', False, 0, 60, 'Days', 'Days',
     'MPR',
     'DIM at first ketosis attention from MPR. Observed range: 0-54. '
     'Free-text values ("Bekapt, RA klos", "Metritis Ketose 2") '
     '-> ClinicalObservationsOverflow prefixed "KetosisAttention1MPR:" + InvalidNumeric warning.',
     'Trim;MoveNonNumericToOverflow;NumericExtract;IntCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # DimAtMPRSampling: observed range 10-42 (single/double digits valid).
    # Excel serials (42834, 42860, etc.) -> NULL + InvalidNumeric warning.
    ('DIMopMementVanMonsteren_59', 'DimAtMPRSampling', 'int', False, 0, 60, 'Days', 'Days',
     'MPR',
     'Days in milk at MPR sampling. Observed valid range: 10-42. '
     'Excel serial values (42834+) -> NULL + InvalidNumeric warning.',
     'Trim;RejectExcelSerial;NumericExtract;IntCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    ('datumMonsternameMPR_60', 'MPRSamplingDate', 'date', False, None, None, None, None,
     'MPR', 'MPR sampling date.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),

    # KetosisAttention2DIM: only 3 non-null values in data.
    # "42887" = Excel serial -> NULL. "Afgevoerd speenbetrapping" = free text -> overflow.
    # "48" = valid DIM value.
    ('TweedeKetoseAttentieDIM_61', 'KetosisAttention2DIM', 'int', False, 0, 600, 'Days', 'Days',
     'MPR',
     'DIM at second ketosis attention from MPR. Only 3 non-null values in data (48, 42887, free text). '
     'Excel serial (42887) -> NULL + InvalidNumeric warning. '
     'Free text ("Afgevoerd speenbetrapping") -> ClinicalObservationsOverflow.',
     'Trim;RejectExcelSerial;MoveNonNumericToOverflow;NumericExtract;IntCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    ('TweedeKetoseAttentieDatumMonsternameMPR_62', 'KetosisAttention2MPRDate', 'date', False, None, None, None, None,
     'MPR', 'Second ketosis MPR sampling date.',
     'MultiDateParse',
     True, _DATE_FMTS, 'Warn', 'AmbiguousDate|FutureDate|OldDate|InvalidDateFormat', None, None),
]
print(f"{DATASET}: {len(reference_rules)} column rules")


# Unit-conversion lookup
reference_unit_conversions = [
    ('CalciumLevel', 'MmolPerL', 'MmolPerL|MgPerDL', 'MgPerDL', 'Value / 4.008', None, None),
    ('BHBA1',        'MmolPerL', 'MmolPerL|MgPerDL', 'MgPerDL', 'Value * 0.0961', None, None),
    ('BHBA2',        'MmolPerL', 'MmolPerL|MgPerDL', 'MgPerDL', 'Value * 0.0961', None, None),
]
print(f"{DATASET}: {len(reference_unit_conversions)} unit-conversion rows")


# Cleaning constraints
reference_constraints = [

    ('CF01', 'Column', '*',
     'TrimWhitespace', '',
     'Fix', '', 'Trim leading/trailing whitespace on all string columns'),

    ('CF02', 'Column', '*',
     'BlankToNull', 'Tokens=|Null|NULL|null|NA|N/A|NaN|nan|-| -|   |.|?',
     'Fix', '', 'Map empty-ish strings, lone dash, lone space, and ? to NULL'),

    ('CF03', 'Column', 'LifeNumber',
     'CollapseSpaces;UpperCase;StripSuffix(*)', 'Derive=LifeNumberDigits',
     'Fix', '', 'Normalise life number; strip asterisk suffix; derive digits-only join key'),

    ('CF04', 'Column', 'HerdIdentifier|AnimalIdentifier|LifeNumber',
     'RejectValues', 'Values=0|00|000|NA|N/A|Nvt|NVT|None|NONE|NULL|null|.',
     'Fix', 'Warning', 'Placeholder identifiers -> NULL + InvalidIdentifier warning'),

    ('CF05', 'Column', '*',
     'NormalizeWhitespace', '',
     'Fix', '', 'Collapse repeated whitespace and normalise Unicode spacing'),

    ('CF06', 'Column', 'CalvingTime',
     'NormalizeTime24h', 'Formats=H:mm:ss|HH:mm:ss|h:mm:ss a|hh:mm:ss a',
     'Fix', 'Warning',
     'Normalise CalvingTime to HH:mm:ss. Step 1: ! -> 1 (typo). '
     'Step 2: +/- prefix kept raw + ApproximateTime warning. '
     'Step 3: AM/PM converted. Step 4: unparseable -> NULL + InvalidTimeFormat warning.'),

    ('RC01', 'Row', 'DryStartScoreDate|DryEndScoreDate|CalvingDate',
     'DateOrder', 'Order=DryStartScoreDate<=DryEndScoreDate<=CalvingDate',
     'Flag', 'Warning', 'Dry-period scoring dates must precede calving date'),

    ('RC02', 'Row', 'CalvingDate|CalciumDate',
     'DateOrder', 'Order=CalvingDate<=CalciumDate',
     'Flag', 'Warning', 'Calcium sample must be on or after calving date'),

    ('RC03', 'Row', 'CalvingDate|BHBA1Date|BHBA2Date',
     'DateOrder', 'Order=CalvingDate<=BHBA1Date<=BHBA2Date',
     'Flag', 'Warning', 'BHBA samples after calving and in chronological sequence'),

    ('RC04', 'Row', 'CalvingDate|ReproExam4Date|ReproExam8Date',
     'DateOrder', 'Order=CalvingDate<=ReproExam4Date<=ReproExam8Date',
     'Flag', 'Warning', 'Reproductive exams after calving; week-4 before week-8'),

    ('RC05', 'Row', 'CalciumDate|CalvingDate|DimCalcium',
     'DimConsistency', 'Date=CalciumDate;Anchor=CalvingDate;Dim=DimCalcium;Tol=3',
     'Flag', 'Warning', 'DimCalcium should equal days between CalvingDate and CalciumDate (+/-3)'),

    ('RC06', 'Row', 'BHBA1Date|CalvingDate|DimBhba1',
     'DimConsistency', 'Date=BHBA1Date;Anchor=CalvingDate;Dim=DimBhba1;Tol=3',
     'Flag', 'Warning', 'DimBhba1 should equal days between CalvingDate and BHBA1Date (+/-3)'),

    ('RC07', 'Row', 'BHBA2Date|CalvingDate|DimBhba2',
     'DimConsistency', 'Date=BHBA2Date;Anchor=CalvingDate;Dim=DimBhba2;Tol=3',
     'Flag', 'Warning', 'DimBhba2 should equal days between CalvingDate and BHBA2Date (+/-3)'),

    ('RC08', 'Row', 'CalvingDate|ExpectedCalvingDate',
     'DateDiffRange', 'A=CalvingDate;B=ExpectedCalvingDate;AbsMax=30',
     'Flag', 'Warning', 'Actual calving should be within 30 days of expected calving date'),

    ('DK01', 'Row', 'HerdIdentifier|AnimalIdentifier|LifeNumber',
     'DedupExact', 'Keys=HerdIdentifier|AnimalIdentifier|LifeNumber',
     'Fix', '',
     'Remove exact duplicates on all three identity keys; keep most-complete row'),

    ('DK02', 'Row', 'HerdIdentifier|AnimalIdentifier',
     'DedupExact', 'Keys=HerdIdentifier|AnimalIdentifier',
     'Fix', '',
     'Deduplicate by herd + animal when LifeNumber is missing; keep most-complete row'),
]
print(f"{DATASET}: {len(reference_constraints)} constraints")


# TranslationGlossary
# Applied in gold to produce boolean condition flag columns across all Dutch text columns.
# ApplicableColumns includes ClinicalObservationsOverflow for every term —
# any Dutch word landing in overflow from any column is still caught in gold.

_ALL = ('Notes|IndividualAttention1|IndividualAttention2|IndividualAttention3'
        '|IndividualAttention4|ClinicalObservationsOverflow')

reference_translation_glossary = [
    # (TermNl, TermEn, Category, FlagColumn, ApplicableColumns, Note)

    # Calving events
    ('afgekalfd',             'calved',                           'Event',        'NoteCalved',                 _ALL, ''),
    ('geboorte',              'calving / birth',                  'Event',        'NoteCalving',                _ALL, ''),
    ('tweeling',              'twins',                            'Event',        'NoteTwins',                  _ALL, ''),
    ('groot kalf',            'large calf',                       'Event',        'NoteLargeCalf',              _ALL, ''),
    ('licht kalf',            'small / light calf',               'Event',        'NoteSmallCalf',              _ALL, ''),
    ('stierkalf',             'bull calf',                        'Event',        'NoteBullCalf',               _ALL, ''),
    ('doodgeboren',           'stillborn',                        'Event',        'NoteStillborn',              _ALL, ''),
    ('stillbirth',            'stillbirth',                       'Event',        'NoteStillborn',              _ALL, 'English cognate'),
    ('verworpen',             'aborted / miscarried',             'Event',        'NoteAbortion',               _ALL, ''),
    ('afgevoerd',             'culled / removed from herd',       'Event',        'NoteCulled',                 _ALL, ''),
    ('afvoer',                'culling / removal',                'Event',        'NoteCulled',                 _ALL, 'Shorter form of afgevoerd'),
    ('afgekeurd',             'rejected / condemned at slaughter','Event',        'NoteCondemned',              _ALL, ''),
    ('dood',                  'dead',                             'Event',        'NoteDead',                   _ALL, ''),
    ('speenbetrapping',       'teat entrapment / calf suckling another cow', 'Event', 'NoteTeatEntrapment',     _ALL, 'Seen in KetosisAttention2DIM overflow'),

    # Interventions
    ('geboortekrik',          'calving jack / obstetric aid',     'Intervention', 'NoteCalvingAid',             _ALL, 'Assisted delivery with traction device'),
    ('verlossing',            'assisted delivery',                'Intervention', 'NoteAssistedDelivery',       _ALL, ''),
    ('keizersnede',           'caesarean section',                'Intervention', 'NoteCaesarean',              _ALL, ''),
    ('trajectie',             'traction',                         'Intervention', 'NoteTraction',               _ALL, ''),
    ('nageboortecapsule',     'placenta capsule (treatment)',      'Intervention', 'NoteRetainedPlacentaTreated',_ALL, ''),
    ('niet behandeld',        'not treated',                      'Intervention', 'NoteNotTreated',             _ALL, 'Check before behandeld to avoid false positive'),
    ('behandeld',             'treated',                          'Intervention', 'NoteTreated',                _ALL, ''),
    ('bekapt',                'hooves trimmed',                   'Intervention', 'NoteHoovesTrimmed',          _ALL, ''),
    ('vervangen',             'replaced',                         'Intervention', '',                           _ALL, ''),

    # Conditions
    ('aan de nageboorte',     'retained placenta',                'Condition',    'NoteRetainedPlacenta',       _ALL, 'Phrase-level; check before nageboorte alone'),
    ('verlengd',              'prolonged',                        'Condition',    'NoteProlonged',              _ALL, ''),
    ('koorts',                'fever',                            'Condition',    'NoteFever',                  _ALL, ''),
    ('coli mastitis',         'coliform mastitis',                'Condition',    'NoteColiMastitis',           _ALL, 'Check before mastitis'),
    ('mastitis',              'mastitis',                         'Condition',    'NoteMastitis',               _ALL, 'Cognate'),
    ('metritis',              'metritis',                         'Condition',    'NoteMetritis',               _ALL, 'Cognate'),
    ('baarmoederontsteking',  'uterine infection / metritis',     'Condition',    'NoteUterineInfection',       _ALL, ''),
    ('endometritis',          'endometritis',                     'Condition',    'NoteEndometritis',           _ALL, 'Cognate'),
    ('ketose',                'ketosis',                          'Condition',    'NoteKetosis',                _ALL, 'Cognate'),
    ('melkziekte',            'milk fever / hypocalcaemia',       'Condition',    'NoteMilkFever',              _ALL, ''),
    ('nabloeding',            'postpartum haemorrhage',           'Condition',    'NotePostpartumHaemorrhage',  _ALL, ''),
    ('infectie',              'infection',                        'Condition',    'NoteInfection',              _ALL, ''),
    ('ontsteking',            'inflammation / infection',         'Condition',    'NoteInflammation',           _ALL, ''),
    ('verminderde eetlust',   'reduced appetite',                 'Condition',    'NoteReducedAppetite',        _ALL, ''),
    ('slechte pensvulling',   'poor rumen fill',                  'Condition',    'NotePoorRumenFill',          _ALL, ''),
    ('zwak',                  'weak',                             'Condition',    'NoteWeak',                   _ALL, ''),
    ('vermoeid',              'exhausted',                        'Condition',    'NoteExhausted',              _ALL, ''),
    ('gedehydrateerd',        'dehydrated',                       'Condition',    'NoteDehydrated',             _ALL, ''),
    ('diarree',               'diarrhoea',                        'Condition',    'NoteDiarrhoea',              _ALL, ''),
    ('darmdraaing',           'intestinal torsion',               'Condition',    'NoteIntestinalTorsion',      _ALL, ''),
    ('darmontsteking',        'intestinal infection / enteritis', 'Condition',    'NoteEnteritis',              _ALL, ''),
    ('kreupel',               'lame',                             'Condition',    'NoteLame',                   _ALL, ''),
    ('ernstig kreupel',       'severely lame',                    'Condition',    'NoteSeverelyLame',           _ALL, 'Check before kreupel'),
    ('tussenklauwontsteking', 'interdigital dermatitis',          'Condition',    'NoteFootInfection',          _ALL, ''),
    ('klauwontsteking',       'claw infection',                   'Condition',    'NoteClawInfection',          _ALL, ''),
    ('zoolzweer',             'sole ulcer',                       'Condition',    'NoteSoleUlcer',              _ALL, ''),
    ('witte lijn defect',     'white line defect',                'Condition',    'NoteWhiteLineDefect',        _ALL, ''),
    ('slakkepoot',            'digital dermatitis',               'Condition',    'NoteDigitalDermatitis',      _ALL, 'Also dermatitis interdigitalis'),
    ('carpus verdikt',        'swollen carpus / knee',            'Condition',    'NoteSwollenJoint',           _ALL, ''),
    ('verscherpt ademen',     'laboured breathing',               'Condition',    '',                           _ALL, ''),
    ('zwakke achterhand',     'weak hindquarters',                'Condition',    '',                           _ALL, ''),
    ('wankelende gang',       'unsteady gait',                    'Condition',    '',                           _ALL, ''),
    ('cyste',                 'cyst (ovarian)',                   'Condition',    'NoteOvarianCyst',            _ALL, ''),
    ('cysteus',               'cystic (ovarian cyst)',            'Condition',    'NoteOvarianCyst',            _ALL, ''),
    ('folliculaire cyste',    'follicular cyst',                  'Condition',    'NoteFollicularCyst',         _ALL, ''),
    ('abces',                 'abscess',                          'Condition',    'NoteAbscess',                _ALL, ''),
    ('afwijkende melk',       'abnormal milk',                    'Condition',    'NoteAbnormalMilk',           _ALL, ''),
    ('witvuilen',             'white discharge (pyometra)',        'Condition',    'NoteWhiteDischarge',         _ALL, ''),
    ('hartklepinfectie',      'heart valve infection / endocarditis','Condition', 'NoteEndocarditis',           _ALL, ''),
    ('bloedvergiftiging',     'blood poisoning / septicaemia',    'Condition',    'NoteSepticaemia',            _ALL, ''),
    ('tumor',                 'tumour',                           'Condition',    'NoteTumour',                 _ALL, 'Seen in Repro8LeftOvary overflow'),

    # Reproduction
    ('dracht',                'pregnancy',                        'Repro',        'NotePregnancy',              _ALL, ''),
    ('drachtig',              'pregnant',                         'Repro',        'NotePregnancy',              _ALL, ''),
    ('vroege dracht',         'early pregnancy',                  'Repro',        'NoteEarlyPregnancy',         _ALL, 'Also seen in CycleWeek8 overflow'),
    ('mogelijk vroege dracht','possibly early pregnancy',         'Repro',        'NoteEarlyPregnancy',         _ALL, 'Seen in CycleWeek8 overflow'),
    ('zwanger',               'pregnant',                         'Repro',        'NotePregnancy',              _ALL, ''),

    # Anatomy
    ('nageboorte',            'afterbirth / placenta',            'Anatomy',      '',                           _ALL, ''),
    ('placenta',              'placenta / afterbirth',            'Anatomy',      '',                           _ALL, ''),
    ('baarmoeder',            'uterus / womb',                    'Anatomy',      '',                           _ALL, ''),
    ('waterblaas',            'amniotic sac / water bag',         'Anatomy',      '',                           _ALL, ''),
    ('pootjes',               'calf feet / legs',                 'Anatomy',      '',                           _ALL, ''),
    ('klauw',                 'claw / hoof',                      'Anatomy',      '',                           _ALL, ''),
    ('heup',                  'hip',                              'Anatomy',      '',                           _ALL, ''),
    ('RA klos',               'right front (RA) lump / clot',     'Anatomy',      '',                           _ALL, 'RA = rechts voor (right front quarter)'),

    # Medications
    ('penicilline',           'penicillin',                       'Medication',   '',                           _ALL, ''),
    ('amprofin',              'ampicillin (amprofin)',             'Medication',   '',                           _ALL, 'Brand name'),
    ('novem',                 'novem (anti-inflammatory)',         'Medication',   '',                           _ALL, 'Brand name'),
    ('dofatrim',              'dofatrim (antibiotic)',             'Medication',   '',                           _ALL, 'Brand name'),
    ('metricure',             'metricure (uterine treatment)',     'Medication',   '',                           _ALL, 'Brand name'),
    ('receptal',              'receptal (GnRH)',                   'Medication',   '',                           _ALL, 'Brand name'),

    # Sensors
    ('pootresponder',         'leg sensor tag',                   'Sensor',       'NoteSensorIssue',            _ALL, ''),
    ('responder',             'sensor tag / responder',           'Sensor',       'NoteSensorIssue',            _ALL, ''),

    # Timing
    ('ongeveer',              'approximately',                     'Timing',       '',                           _ALL, ''),
    ('zichtbaar',             'visible',                          'Observation',  '',                           _ALL, ''),
    ('s nachts',              'at night',                         'Timing',       '',                           _ALL, ''),
    ('s ochtends',            'in the morning',                   'Timing',       '',                           _ALL, ''),
    ('verdenking',            'suspected',                        'Qualifier',    '',                           _ALL, ''),

    # Admin
    ('afkalfformulier',       'calving form (admin)',              'Admin',        '',                           _ALL, ''),
    ('formulier ontbreekt',   'form missing',                     'Admin',        'NoteFormMissing',            _ALL, ''),
    ('vermist',               'missing',                          'Admin',        '',                           _ALL, ''),
    ('missend',               'missing',                          'Admin',        '',                           _ALL, ''),
]
print(f"{len(reference_translation_glossary)} translation glossary terms")

# Code mappings
reference_code_mappings = [
    ('CalvingType', '1', 'Normal',     'Unassisted calving'),
    ('CalvingType', '2', 'Assisted',   'Manual assistance required'),
    ('CalvingType', '3', 'Difficult',  'Significant difficulty'),
    ('CalvingType', '4', 'Caesarean',  'Caesarean section'),
    ('CalvingType', '5', 'Stillbirth', 'Calf born dead'),

    ('CalvingSignal', '1', 'NoSignal',          'PROVISIONAL'),
    ('CalvingSignal', '2', 'TailUp',            'PROVISIONAL'),
    ('CalvingSignal', '3', 'SensorAlert',       'PROVISIONAL'),
    ('CalvingSignal', '4', 'VisualCheck',       'PROVISIONAL'),
    ('CalvingSignal', '5', 'FarmerObservation', 'PROVISIONAL'),
    ('CalvingSignal', '6', 'Other',             'PROVISIONAL'),

    ('CalfRemoval', '1', 'Immediate',        'PROVISIONAL'),
    ('CalfRemoval', '2', 'AfterColostrum',   'PROVISIONAL'),
    ('CalfRemoval', '3', 'After1Day',        'PROVISIONAL'),
    ('CalfRemoval', '4', 'AfterSeveralDays', 'PROVISIONAL'),
    ('CalfRemoval', '5', 'CalfDied',         'PROVISIONAL'),
    ('CalfRemoval', '6', 'Other',            'PROVISIONAL'),

    ('Afterbirth', '1', 'NormalShedWithin8h',      'Placenta shed normally within 8 hours'),
    ('Afterbirth', '2', 'RetainedShedNaturally',   'Retained > 8h, shed without intervention'),
    ('Afterbirth', '3', 'RetainedManuallyRemoved', 'Retained, manually removed'),
    ('Afterbirth', '4', 'RetainedNotRemoved',      'Retained, not removed'),

    ('Repro4Uterus', 'S', 'Normal',     'Uterus normal / fully involuted'),
    ('Repro4Uterus', 'A', 'Abnormal',   'Abnormal uterine finding'),
    ('Repro4Uterus', 'I', 'Involuting', 'Still involuting'),
    ('Repro4Uterus', 'V', 'Other',      'Other finding'),
    ('Repro8Uterus', 'S', 'Normal',     'Uterus normal / fully involuted'),
    ('Repro8Uterus', 'A', 'Abnormal',   'Abnormal uterine finding'),
    ('Repro8Uterus', 'I', 'Involuting', 'Still involuting'),
    ('Repro8Uterus', 'V', 'Other',      'Other finding'),
]
print(f"{DATASET}: {len(reference_code_mappings)} code-mapping rows")


# Persist all tables
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, BooleanType)

def replace_dataset(df, table):
    fq = f"{SILVER_SCHEMA}.{table}"
    if spark.catalog.tableExists(fq):
        spark.sql(f"DELETE FROM {fq} WHERE Dataset = '{DATASET}'")
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(fq)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq)
    print(f"  {table}: {df.count()} rows for {DATASET}")

RULE_COLS = [
    "SourceColumn", "TargetColumn", "DataType", "IsRequired",
    "MinValue", "MaxValue", "CanonicalUnit", "AllowedUnits",
    "SemanticGroup", "Description", "CleaningStrategy",
    "AcceptedFormats", "InvalidAction", "WarningType", "Pattern", "AllowedValues",
]
rule_schema = StructType(
    [StructField("Dataset", StringType()), StructField("Ordinal", IntegerType())]
    + [StructField(c, StringType() if c not in ("IsRequired", "MinValue", "MaxValue")
                   else (BooleanType() if c == "IsRequired" else DoubleType()))
       for c in RULE_COLS]
)
rule_data = [
    (DATASET, i,
     r[0], r[1], r[2], r[3],
     None if r[4] is None else float(r[4]),
     None if r[5] is None else float(r[5]),
     r[6], r[7], r[8], r[9],
     r[10], r[12], r[13], r[14], r[15], r[16])
    for i, r in enumerate(reference_rules)
]
replace_dataset(spark.createDataFrame(rule_data, rule_schema), "Rulebook")

conv_schema = StructType([StructField(n, StringType()) for n in
    ["Dataset", "ColumnName", "CanonicalUnit", "AllowedUnits", "Unit1", "Formula1", "Unit2", "Formula2"]])
replace_dataset(spark.createDataFrame(
    [(DATASET, r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in reference_unit_conversions],
    conv_schema), "UnitConversionLookup")

cons_schema = StructType([StructField(n, StringType()) for n in
    ["Dataset", "ConstraintId", "Scope", "TargetColumn",
     "ConstraintType", "Params", "Action", "Severity", "Description"]])
replace_dataset(spark.createDataFrame(
    [(DATASET, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in reference_constraints],
    cons_schema), "CleaningConstraints")

gloss_schema = StructType([StructField(n, StringType()) for n in
    ["Dataset", "TermNl", "TermEn", "Category", "FlagColumn", "ApplicableColumns", "Note"]])
replace_dataset(spark.createDataFrame(
    [(DATASET, r[0], r[1], r[2], r[3], r[4], r[5]) for r in reference_translation_glossary],
    gloss_schema), "TranslationGlossary")

code_schema = StructType([StructField(n, StringType()) for n in
    ["Dataset", "Column", "Code", "Label", "Description"]])
replace_dataset(spark.createDataFrame(
    [(DATASET, r[0], r[1], r[2], r[3]) for r in reference_code_mappings],
    code_schema), "CodeMappings")

for t in ["Rulebook", "UnitConversionLookup", "CleaningConstraints",
          "TranslationGlossary", "CodeMappings"]:
    print(f"{t}")
    display(spark.table(f"{SILVER_SCHEMA}.{t}").where(f"Dataset='{DATASET}'").limit(50))

print("Reference data rulebook written to:", SILVER_SCHEMA)