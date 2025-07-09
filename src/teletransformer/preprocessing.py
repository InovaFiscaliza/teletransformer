import re
import string

# regex for match numbers with lenght greather than or equal to 10
E164_FULL_NUMBERS = re.compile(
    r"""# (BRAZIL COUNTRY CODE) (CSP)
        ^(?:55)?(?:1[2-8]|2[12469]|3[16789]|4[1235679]|5[3568]|6[1235]|7[12456]|8[157]|9[18])?(
            # CN+PREFIXO+MCDU
            # SMP
            (?:1[1-9]9[0-9]{8})$|
            (?:2[12478]9[0-9]{8})$|
            (?:3[1-578]9[0-9]{8})$|
            (?:4[1-9]9[0-9]{8})$|
            (?:5[1345]9[0-9]{8})$|
            (?:6[1-9]9[0-9]{8})$|
            (?:7[134579]9[0-9]{8})$|
            (?:8[1-9]9[0-9]{8})$|
            (?:9[1-9]9[0-9]{8})$|
            # STFC
            (?:1[1-9][2345][0-9]{7})$|
            (?:2[12478][2345][0-9]{7})$|
            (?:3[1-578][2345][0-9]{7})$|
            (?:4[1-9][2345][0-9]{7})$|
            (?:5[1345][2345][0-9]{7})$|
            (?:6[1-9][2345][0-9]{7})$|
            (?:7[13457][2345][0-9]{7})$|
            (?:8[1-9][2345][0-9]{7})$|
            (?:9[1-9][2345][0-9]{7})$|
            # CNG
            (?:0?[589]00[0-9]{7})$|
            (?:0?30[03][0-9]{7})$|
            # SME
            (?:1[1-9]7[0789][0-9]{6})$|
            (?:2[124]7[078][0-9]{6})$|
            (?:2778[0-9]{6})$|
            (?:3[147]7[78][0-9]{6})$|
            (?:4[1-478]78[0-9]{6})$|
            (?:5[14]78[0-9]{6})$|
            (?:6[125]78[0-9]{6})$|
            (?:7[135]78[0-9]{6})$|
            (?:8[15]78[0-9]{6})$
        )""",
    re.VERBOSE,
)

# for match numbers with lenght less than or equal to 9
SMALL_NUMBERS = re.compile(
    r"""(
            # PREFIXO+MCDU
            # SMP
            (?:^9[0-9]{8})$|
            # STFC
            (?:^[2345][0-9]{7})$|
            # SME
            (?:^7[0789][0-9]{6})$|
            # SUP
            (?:^10[024])$|
            (?:^1031[234579])$|
            (?:^1032[13-9])$|
            (?:^1033[124-9])$|
            (?:^1034[123578])$|
            (?:^1035[1-468])$|
            (?:^1036[139])$|
            (?:^1038[149])$|
            (?:^1039[168])$|          
            (?:^105[012356789])$|
            (?:^106[012467])$|
            (?:^1061[0-35-8])$|
            (?:^1062[0145])$|
            (?:^1063[0137])$|
            (?:^1064[4789])$|
            (?:^1065[01235])$|
            (?:^1066[016])$|
            (?:^1067[137])$|
            (?:^1068[5-8])$|
            (?:^1069[1359])$|
            (?:^11[125-8])$|
            (?:^12[135789])$|
            (?:^13[024568])$|
            (?:^133[12])$|
            (?:^1358)$|
            (?:^14[25678])$|
            (?:^15[0-9])$|
            (?:^16[0-8])$|
            (?:^18[0158])$|
            (?:^1746)$|
            (?:^19[0-9])$|
            (?:^911)$
        )""",
    re.VERBOSE,
)


def clean_numbers(text):
    letters = string.ascii_letters
    punctuation = string.punctuation
    remove_table = str.maketrans("", "", letters + punctuation)
    return text.translate(remove_table)


def normalize_number(original_subscriber_number, national_destination_code=""):
    original_subscriber_number = str(original_subscriber_number)
    if ";" in original_subscriber_number:
        original_subscriber_number = original_subscriber_number.split(";")[0]

    normalized_subscriber_number = clean_numbers(original_subscriber_number)
    # remove collect call indicator or the international/national prefix
    normalized_subscriber_number = re.sub(
        "^9090|^00|^0", "", normalized_subscriber_number
    )

    # https://handle.itu.int/11.1002/1000/10688
    # 6.1 International ITU-T E.164-number length
    # ITU-T recommends that the maximum number of digits for the international geographic, global
    # services, Network and groups of countries applications should be 15 (excluding the international
    # prefix).
    if len(normalized_subscriber_number) > 15:
        return [original_subscriber_number, False]

    if len(normalized_subscriber_number) >= 10:
        normalized_subscriber_number = E164_FULL_NUMBERS.findall(
            normalized_subscriber_number
        )
    else:
        normalized_subscriber_number = SMALL_NUMBERS.findall(
            normalized_subscriber_number
        )

    if len(normalized_subscriber_number) == 1:
        normalized_subscriber_number = normalized_subscriber_number[0]
        if len(normalized_subscriber_number) in (8, 9) and national_destination_code:
            normalized_subscriber_number = (
                f"{national_destination_code}{normalized_subscriber_number}"
            )
        return [normalized_subscriber_number, True]

    return [original_subscriber_number, False]


def extract_normalized_numbers(number_a, number_b):
    normalized_number_a, is_number_a_valid = normalize_number(number_a)

    if is_number_a_valid and len(normalized_number_a) in (10, 11):
        national_destination_code = normalized_number_a[:2]
    else:
        national_destination_code = ""

    normalized_number_b, is_number_b_valid = normalize_number(
        number_b, national_destination_code
    )

    return (
        normalized_number_a,
        is_number_a_valid,
        normalized_number_b,
        is_number_b_valid,
    )
