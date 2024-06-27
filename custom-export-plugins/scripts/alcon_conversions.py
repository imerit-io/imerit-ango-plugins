import json
import os
import uuid
import copy


def alcon_conversions(**data):
    project_id = data.get("projectId")

    # Get json export from data response
    json_export = data.get("jsonExport")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running alcon_conversions script on Project: {project_id}")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    new_collection = []
    asset_counter = 1
    wrong_filesnames = {
    "7d397e0e4f114dcc9b5cae482e217356": "ref_CTT644E001_exportID_0161_pair_0464$1280x1024+surg_CTT644E001_exportID_0161_pair_0464$1024x1280.JPG",
    "27edfdb3b3374d099185e9bed18a93e8": "ref_CTT644E001_exportID_0145_pair_0411$1280x1024+surg_CTT644E001_exportID_0145_pair_0411$1280x1024.JPG",
    "ecec796c3cd740e48863b95f326bcfe5": "ref_CTT644E001_exportID_0144_pair_0409$1280x1024+surg_CTT644E001_exportID_0144_pair_0409$1280x1024.JPG",
    "f9a02b2641f34f2bb967e76ccc9d61f2": "ref_CTT644E001_exportID_0166_pair_0483$1280x1024+surg_CTT644E001_exportID_0166_pair_0483$1024x1280.JPG",
    "0981fe133be84bc7a21b40a6e6f9ac97": "ref_CTT644E001_exportID_0159_pair_0462$1280x1024+surg_CTT644E001_exportID_0159_pair_0462$1024x1280.JPG",
    "d8f5253eaf0346088675bb4661a88258": "ref_CTT644E001_exportID_0153_pair_0440$1280x1024+surg_CTT644E001_exportID_0153_pair_0440$1024x1280.JPG",
    "7b3722ff192541e8920fac4419dffb85": "ref_CTT644E001_exportID_0195_pair_0576$1280x1024+surg_CTT644E001_exportID_0195_pair_0576$1280x1024.JPG",
    "37912792157343dfa964260fc09b5c03": "ref_CTT644E001_exportID_0170_pair_0500$1280x1024+surg_CTT644E001_exportID_0170_pair_0500$1024x1280.JPG",
    "47a8db5947d34ab78b8c02b0db10427e": "ref_CTT644E001_exportID_0171_pair_0505$1280x1024+surg_CTT644E001_exportID_0171_pair_0505$1024x1280.JPG",
    "c2c9cf8bd73f4718b14f5da099578af5": "ref_CTT644E001_exportID_0155_pair_0447$1280x1024+surg_CTT644E001_exportID_0155_pair_0447$1024x1280.JPG",
    "8eaa44b2bc284bcf956abdcf051a6eda": "ref_CTT644E001_exportID_0153_pair_0439$1280x1024+surg_CTT644E001_exportID_0153_pair_0439$1024x1280.JPG",
    "73a889107d434f89bfbdfefe5045cf9e": "ref_CTT644E001_exportID_0155_pair_0449$1280x1024+surg_CTT644E001_exportID_0155_pair_0449$1024x1280.JPG",
    "2f828aa0ce0c4a3e93b35d00b2814cee": "ref_CTT644E001_exportID_0186_pair_0553$1280x1024+surg_CTT644E001_exportID_0186_pair_0553$1024x1280.JPG",
    "a624d4b5520240a881d46046fd2ecbe9": "ref_CTT644E001_exportID_0163_pair_0473$1280x1024+surg_CTT644E001_exportID_0163_pair_0473$1024x1280.JPG",
    "c2917ce75b3c4893a189fd149d1dba4b": "ref_CTT644E001_exportID_0165_pair_0480$1280x1024+surg_CTT644E001_exportID_0165_pair_0480$1024x1280.JPG",
    "1710946af7bb44039630d46ee893317f": "ref_CTT644E001_exportID_0204_pair_0596$1280x1024+surg_CTT644E001_exportID_0204_pair_0596$1280x1024.JPG",
    "4c5fd8c14a504ce78b7e9bfdcdac296f": "ref_CTT644E001_exportID_0171_pair_0503$1280x1024+surg_CTT644E001_exportID_0171_pair_0503$1024x1280.JPG",
    "8ff431d227824876be85cf29f9625cd1": "ref_CTT644E001_exportID_0147_pair_0417$1280x1024+surg_CTT644E001_exportID_0147_pair_0417$1024x1280.JPG",
    "569adca71f9443139859d415573f3dd5": "ref_CTT644E001_exportID_0186_pair_0554$1280x1024+surg_CTT644E001_exportID_0186_pair_0554$1024x1280.JPG",
    "18deb5a191874370b8d72519bf594cfd": "ref_CTT644E001_exportID_0186_pair_0551$1280x1024+surg_CTT644E001_exportID_0186_pair_0551$1024x1280.JPG",
    "1024f1cbfe134ce5b1f241122d5ed82d": "ref_CTT644E001_exportID_0203_pair_0592$1280x1024+surg_CTT644E001_exportID_0203_pair_0592$1280x1024.JPG",
    "1e1f993964e24760b4792cade043b436": "ref_CTT644E001_exportID_0204_pair_0597$1280x1024+surg_CTT644E001_exportID_0204_pair_0597$1280x1024.JPG",
    "1971f7266ca645a7a2417d25ab1adc1c": "ref_CTT644E001_exportID_0189_pair_0560$1280x1024+surg_CTT644E001_exportID_0189_pair_0560$1280x1024.JPG",
    "9bf546a28051473abc9d7d929102357d": "ref_CTT644E001_exportID_0170_pair_0501$1280x1024+surg_CTT644E001_exportID_0170_pair_0501$1024x1280.JPG",
    "190b371f5263448a984f62a3b3b719a1": "ref_CTT644E001_exportID_0144_pair_0410$1280x1024+surg_CTT644E001_exportID_0144_pair_0410$1280x1024.JPG",
    "3cb6933b5977439fb1331582e520c764": "ref_CTT644E001_exportID_0188_pair_0557$1280x1024+surg_CTT644E001_exportID_0188_pair_0557$1024x1280.JPG",
    "706692fb1b994a14a5a039a1f0411c0e": "ref_CTT644E001_exportID_0180_pair_0542$1280x1024+surg_CTT644E001_exportID_0180_pair_0542$1024x1280.JPG",
    "b66733293632492a8e31dc9554ac07fb": "ref_CTT644E001_exportID_0148_pair_0420$1280x1024+surg_CTT644E001_exportID_0148_pair_0420$1024x1280.JPG",
    "73288c2f8cff4fc3be1007c009f2d268": "ref_CTT644E001_exportID_0161_pair_0463$1280x1024+surg_CTT644E001_exportID_0161_pair_0463$1024x1280.JPG",
    "41744302d9de408cba523a67cd79d1c4": "ref_CTT644E001_exportID_0167_pair_0488$1280x1024+surg_CTT644E001_exportID_0167_pair_0488$1024x1280.JPG",
    "f4443b0962254b1f921c7768c37afeca": "ref_CTT644E001_exportID_0186_pair_0552$1280x1024+surg_CTT644E001_exportID_0186_pair_0552$1024x1280.JPG",
    "62ecbe0ada594cdb864dc17e87b97239": "ref_CTT644E001_exportID_0199_pair_0583$1280x1024+surg_CTT644E001_exportID_0199_pair_0583$1280x1024.JPG",
    "a1a3700e0c3846db92d30fd0f9c6f7ee": "ref_CTT644E001_exportID_0152_pair_0435$1280x1024+surg_CTT644E001_exportID_0152_pair_0435$1024x1280.JPG",
    "097bd42aa1af4e16bafa58eea19a9079": "ref_CTT644E001_exportID_0169_pair_0498$1280x1024+surg_CTT644E001_exportID_0169_pair_0498$1024x1280.JPG",
    "9834405a489d448f97f455ad15458369": "ref_CTT644E001_exportID_0194_pair_0574$1280x1024+surg_CTT644E001_exportID_0194_pair_0574$1280x1024.JPG",
    "b5848a9b6d23456489efa0d2c8f91597": "ref_CTT644E001_exportID_0193_pair_0569$1280x1024+surg_CTT644E001_exportID_0193_pair_0569$1280x1024.JPG",
    "c7bde80720bf44e0bcddaffc4402d02d": "ref_CTT644E001_exportID_0154_pair_0444$1280x1024+surg_CTT644E001_exportID_0154_pair_0444$1024x1280.JPG",
    "18da29f2bbdf4d609bd7a11f2e39e1d7": "ref_CTT644E001_exportID_0179_pair_0535$1280x1024+surg_CTT644E001_exportID_0179_pair_0535$1024x1280.JPG",
    "aef2aa72ba2943da8290331b08aaa992": "ref_CTT644E001_exportID_0164_pair_0477$1280x1024+surg_CTT644E001_exportID_0164_pair_0477$1024x1280.JPG",
    "4045b71c9e4c4adab36a287046d48ae8": "ref_CTT644E001_exportID_0203_pair_0594$1280x1024+surg_CTT644E001_exportID_0203_pair_0594$1280x1024.JPG",
    "931bec05cd9344b1b5cc35f1536d707d": "ref_CTT644E001_exportID_0157_pair_0457$1280x1024+surg_CTT644E001_exportID_0157_pair_0457$1024x1280.JPG",
    "cbe1b5d31cca48f294f3fa2125174139": "ref_CTT644E001_exportID_0167_pair_0487$1280x1024+surg_CTT644E001_exportID_0167_pair_0487$1024x1280.JPG",
    "c2821b02196f47ae8520e62911c9292e": "ref_CTT644E001_exportID_0149_pair_0426$1280x1024+surg_CTT644E001_exportID_0149_pair_0426$1024x1280.JPG",
    "6518c8f75f91480aa6f786a75262b9e7": "ref_CTT644E001_exportID_0150_pair_0427$1280x1024+surg_CTT644E001_exportID_0150_pair_0427$1024x1280.JPG",
    "122ed64eae674bd3a3e9109811664728": "ref_CTT644E001_exportID_0188_pair_0558$1280x1024+surg_CTT644E001_exportID_0188_pair_0558$1024x1280.JPG",
    "f43e30231e4c4c529c36bf10591be6f0": "ref_CTT644E001_exportID_0200_pair_0587$1280x1024+surg_CTT644E001_exportID_0200_pair_0587$1280x1024.JPG",
    "c9211696012f4216b4638802a8239921": "ref_CTT644E001_exportID_0181_pair_0544$1280x1024+surg_CTT644E001_exportID_0181_pair_0544$1024x1280.JPG",
    "d1c459dd515f4b029e687267309cd585": "ref_CTT644E001_exportID_0143_pair_0405$1280x1024+surg_CTT644E001_exportID_0143_pair_0405$1280x1024.JPG",
    "fbe13ab389ed43e0afac2811554acce5": "ref_CTT644E001_exportID_0166_pair_0484$1280x1024+surg_CTT644E001_exportID_0166_pair_0484$1024x1280.JPG",
    "a6b60ac519a74864abdb81fec0bc2fe8": "ref_CTT644E001_exportID_0178_pair_0532$1280x1024+surg_CTT644E001_exportID_0178_pair_0532$1024x1280.JPG",
    "af2a265c67cf4ba2857e4b85d590f56b": "ref_CTT644E001_exportID_0176_pair_0523$1280x1024+surg_CTT644E001_exportID_0176_pair_0523$1024x1280.JPG",
    "e448daa702ad41c4aae815b212a37d68": "ref_CTT644E001_exportID_0166_pair_0486$1280x1024+surg_CTT644E001_exportID_0166_pair_0486$1024x1280.JPG",
    "67f1273081ae4076852964e2dac7b25f": "ref_CTT644E001_exportID_0181_pair_0546$1280x1024+surg_CTT644E001_exportID_0181_pair_0546$1024x1280.JPG",
    "f604004418b84ec3a912a97a57381ad1": "ref_CTT644E001_exportID_0152_pair_0437$1280x1024+surg_CTT644E001_exportID_0152_pair_0437$1024x1280.JPG",
    "a53dd556a21b410c8031948bed73da54": "ref_CTT644E001_exportID_0170_pair_0502$1280x1024+surg_CTT644E001_exportID_0170_pair_0502$1024x1280.JPG",
    "edd27fafa705498591f93ee888e37947": "ref_CTT644E001_exportID_0151_pair_0434$1280x1024+surg_CTT644E001_exportID_0151_pair_0434$1024x1280.JPG",
    "42703065466741258eccf5c8362ad2e9": "ref_CTT644E001_exportID_0173_pair_0513$1280x1024+surg_CTT644E001_exportID_0173_pair_0513$1024x1280.JPG",
    "8f1b10d7ad3b4721b816aa8b6c8e4912": "ref_CTT644E001_exportID_0152_pair_0436$1280x1024+surg_CTT644E001_exportID_0152_pair_0436$1024x1280.JPG",
    "233b4b256c324cc1a33d30836c99cefc": "ref_CTT644E001_exportID_0144_pair_0408$1280x1024+surg_CTT644E001_exportID_0144_pair_0408$1280x1024.JPG",
    "ddbf24a5b6f44b20b6de6da05fc17c6d": "ref_CTT644E001_exportID_0165_pair_0479$1280x1024+surg_CTT644E001_exportID_0165_pair_0479$1024x1280.JPG",
    "b531b09e657d46f6a1260f7d66f65d39": "ref_CTT644E001_exportID_0185_pair_0548$1280x1024+surg_CTT644E001_exportID_0185_pair_0548$1024x1280.JPG",
    "5da941a6dd404decb3f891bcc99cecc2": "ref_CTT644E001_exportID_0177_pair_0529$1280x1024+surg_CTT644E001_exportID_0177_pair_0529$1024x1280.JPG",
    "52d8ffd5db084755836cd0cdbeac66ec": "ref_CTT644E001_exportID_0193_pair_0570$1280x1024+surg_CTT644E001_exportID_0193_pair_0570$1280x1024.JPG",
    "36b39081fa14424da71af349f29eaa8d": "ref_CTT644E001_exportID_0173_pair_0511$1280x1024+surg_CTT644E001_exportID_0173_pair_0511$1024x1280.JPG",
    "a541056d78a642568c6643f20ca18f19": "ref_CTT644E001_exportID_0154_pair_0443$1280x1024+surg_CTT644E001_exportID_0154_pair_0443$1024x1280.JPG",
    "edaad6f771cf43c0904881488386c2c7": "ref_CTT644E001_exportID_0155_pair_0448$1280x1024+surg_CTT644E001_exportID_0155_pair_0448$1024x1280.JPG",
    "ac1bad0542ea4650a06f6157ee646177": "ref_CTT644E001_exportID_0150_pair_0429$1280x1024+surg_CTT644E001_exportID_0150_pair_0429$1024x1280.JPG",
    "87fc46f921964b3a99ad87785bdc8fcd": "ref_CTT644E001_exportID_0173_pair_0512$1280x1024+surg_CTT644E001_exportID_0173_pair_0512$1024x1280.JPG",
    "de9823bee2604fec8f260b6e55959e06": "ref_CTT644E001_exportID_0194_pair_0573$1280x1024+surg_CTT644E001_exportID_0194_pair_0573$1280x1024.JPG",
    "bec32f83df9a4a1f8f9ce2952cdef908": "ref_CTT644E001_exportID_0162_pair_0467$1280x1024+surg_CTT644E001_exportID_0162_pair_0467$1024x1280.JPG",
    "7c4cb067b7474f8c87970ec96ca13881": "ref_CTT644E001_exportID_0204_pair_0595$1280x1024+surg_CTT644E001_exportID_0204_pair_0595$1280x1024.JPG",
    "d2da10a48f834fd0a3072eb3627133a9": "ref_CTT644E001_exportID_0185_pair_0549$1280x1024+surg_CTT644E001_exportID_0185_pair_0549$1024x1280.JPG",
    "d1b196ca29f14525aa1b227ae5853d04": "ref_CTT644E001_exportID_0200_pair_0589$1280x1024+surg_CTT644E001_exportID_0200_pair_0589$1280x1024.JPG",
    "a942f0c873ec4249a2fcc5e092f6b3b3": "ref_CTT644E001_exportID_0156_pair_0452$1280x1024+surg_CTT644E001_exportID_0156_pair_0452$1024x1280.JPG",
    "731df028fe0749ebaac4e2175f8ac5ff": "ref_CTT644E001_exportID_0156_pair_0451$1280x1024+surg_CTT644E001_exportID_0156_pair_0451$1024x1280.JPG",
    "35f3a07098ad477d9d7adef2fee73239": "ref_CTT644E001_exportID_0150_pair_0428$1280x1024+surg_CTT644E001_exportID_0150_pair_0428$1024x1280.JPG",
    "f67a1e2bfd704f3fb50907fa42ad5254": "ref_CTT644E001_exportID_0199_pair_0584$1280x1024+surg_CTT644E001_exportID_0199_pair_0584$1280x1024.JPG",
    "e2f62e3413a440258f5d77888c0e288b": "ref_CTT644E001_exportID_0154_pair_0445$1280x1024+surg_CTT644E001_exportID_0154_pair_0445$1024x1280.JPG",
    "e61d0388b8cc4d66b26914a5aef9d344": "ref_CTT644E001_exportID_0177_pair_0527$1280x1024+surg_CTT644E001_exportID_0177_pair_0527$1024x1280.JPG",
    "05bcac8610244e1484e2630c5b430dc3": "ref_CTT644E001_exportID_0149_pair_0423$1280x1024+surg_CTT644E001_exportID_0149_pair_0423$1024x1280.JPG",
    "52bc4af13f5f4955bf6c1fa53558d584": "ref_CTT644E001_exportID_0144_pair_0407$1280x1024+surg_CTT644E001_exportID_0144_pair_0407$1280x1024.JPG",
    "3ce31e0fc696480993b46e2aec9da8ca": "ref_CTT644E001_exportID_0148_pair_0419$1280x1024+surg_CTT644E001_exportID_0148_pair_0419$1024x1280.JPG",
    "76482d7ed7514419928c47787f1559fd": "ref_CTT644E001_exportID_0152_pair_0438$1280x1024+surg_CTT644E001_exportID_0152_pair_0438$1024x1280.JPG",
    "590d7bf45a0347f09d84980227c0dd8a": "ref_CTT644E001_exportID_0178_pair_0531$1280x1024+surg_CTT644E001_exportID_0178_pair_0531$1024x1280.JPG",
    "433130b725a04715a25c9b95f5d2d355": "ref_CTT644E001_exportID_0155_pair_0450$1280x1024+surg_CTT644E001_exportID_0155_pair_0450$1024x1280.JPG",
    "f914ccd5f36342b1a9d370f5164fd9a8": "ref_CTT644E001_exportID_0153_pair_0441$1280x1024+surg_CTT644E001_exportID_0153_pair_0441$1024x1280.JPG",
    "acd3b32d65a7422ab3635a202c6f762a": "ref_CTT644E001_exportID_0147_pair_0415$1280x1024+surg_CTT644E001_exportID_0147_pair_0415$1024x1280.JPG",
    "a401a681f41749b0a9dc696a42d638bc": "ref_CTT644E001_exportID_0200_pair_0588$1280x1024+surg_CTT644E001_exportID_0200_pair_0588$1280x1024.JPG",
    "cca24134d06640c4839aecbb3d26566f": "ref_CTT644E001_exportID_0157_pair_0455$1280x1024+surg_CTT644E001_exportID_0157_pair_0455$1024x1280.JPG",
    "9de47343aa994541889ab7f7cffe9889": "ref_CTT644E001_exportID_0145_pair_0412$1280x1024+surg_CTT644E001_exportID_0145_pair_0412$1280x1024.JPG",
    "c6d7840163d24819987378cc0c2318b2": "ref_CTT644E001_exportID_0198_pair_0580$1280x1024+surg_CTT644E001_exportID_0198_pair_0580$1280x1024.JPG",
    "68d59dd04fea49e5a680392e47a73591": "ref_CTT644E001_exportID_0171_pair_0504$1280x1024+surg_CTT644E001_exportID_0171_pair_0504$1024x1280.JPG",
    "ee52cb01467145e6a0d719fd31983e39": "ref_CTT644E001_exportID_0174_pair_0518$1280x1024+surg_CTT644E001_exportID_0174_pair_0518$1024x1280.JPG",
    "3691e9a441c74532838dd4b179d2876a": "ref_CTT644E001_exportID_0181_pair_0545$1280x1024+surg_CTT644E001_exportID_0181_pair_0545$1024x1280.JPG",
    "c90b34d201a24a7fbdba858d00818959": "ref_CTT644E001_exportID_0145_pair_0413$1280x1024+surg_CTT644E001_exportID_0145_pair_0413$1280x1024.JPG",
    "313d03f43d764427a89188b739783c4f": "ref_CTT644E001_exportID_0179_pair_0536$1280x1024+surg_CTT644E001_exportID_0179_pair_0536$1024x1280.JPG",
    "f8b16f392055431a8cef78f18b942484": "ref_CTT644E001_exportID_0149_pair_0425$1280x1024+surg_CTT644E001_exportID_0149_pair_0425$1024x1280.JPG",
    "801843eccd93495ab116c3ab38d88679": "ref_CTT644E001_exportID_0167_pair_0490$1280x1024+surg_CTT644E001_exportID_0167_pair_0490$1024x1280.JPG",
    "d39f11006e104c279aa292e816bb2a15": "ref_CTT644E001_exportID_0165_pair_0481$1280x1024+surg_CTT644E001_exportID_0165_pair_0481$1024x1280.JPG",
    "27bf5bab1c904b92a9d72d80e8f8b7ef": "ref_CTT644E001_exportID_0157_pair_0456$1280x1024+surg_CTT644E001_exportID_0157_pair_0456$1024x1280.JPG",
    "168c303c59f9411dbf489347e0655381": "ref_CTT644E001_exportID_0154_pair_0446$1280x1024+surg_CTT644E001_exportID_0154_pair_0446$1024x1280.JPG",
    "1d07677222f3426c89a05bf5ce4c31c3": "ref_CTT644E001_exportID_0171_pair_0506$1280x1024+surg_CTT644E001_exportID_0171_pair_0506$1024x1280.JPG",
    "182cc0776a42484bb678400fee953f5b": "ref_CTT644E001_exportID_0147_pair_0418$1280x1024+surg_CTT644E001_exportID_0147_pair_0418$1024x1280.JPG",
    "af1febd99ffd4899ba385dba8f2d3bdc": "ref_CTT644E001_exportID_0143_pair_0404$1280x1024+surg_CTT644E001_exportID_0143_pair_0404$1280x1024.JPG",
    "7ba799373d244c01978f7bc33caa9063": "ref_CTT644E001_exportID_0161_pair_0466$1280x1024+surg_CTT644E001_exportID_0161_pair_0466$1024x1280.JPG",
    "7c1f9f6b1c70452b93b35be9d4e7f34d": "ref_CTT644E001_exportID_0156_pair_0453$1280x1024+surg_CTT644E001_exportID_0156_pair_0453$1024x1280.JPG",
    "210be327c13e448b9821381ede5f92a7": "ref_CTT644E001_exportID_0174_pair_0516$1280x1024+surg_CTT644E001_exportID_0174_pair_0516$1024x1280.JPG",
    "b379d36044424b8e91cbb33106474901": "ref_CTT644E001_exportID_0163_pair_0474$1280x1024+surg_CTT644E001_exportID_0163_pair_0474$1024x1280.JPG",
    "e73791dda6164227bcce28b335f179e2": "ref_CTT644E001_exportID_0151_pair_0433$1280x1024+surg_CTT644E001_exportID_0151_pair_0433$1024x1280.JPG",
    "a6ab81853759494f8a10049c88c751ef": "ref_CTT644E001_exportID_0179_pair_0537$1280x1024+surg_CTT644E001_exportID_0179_pair_0537$1024x1280.JPG",
    "57c989ac36ab43c7b7b963ecbb1b2e3f": "ref_CTT644E001_exportID_0204_pair_0598$1280x1024+surg_CTT644E001_exportID_0204_pair_0598$1280x1024.JPG",
    "306fa3c11cbd495f97ed1f998c2bccd4": "ref_CTT644E001_exportID_0163_pair_0472$1280x1024+surg_CTT644E001_exportID_0163_pair_0472$1024x1280.JPG",
    "c3147994c9e34fa89b66e805ab039c57": "ref_CTT644E001_exportID_0172_pair_0507$1280x1024+surg_CTT644E001_exportID_0172_pair_0507$1024x1280.JPG",
    "69e951ed2fde4132af3d9eaa970541bf": "ref_CTT644E001_exportID_0180_pair_0540$1280x1024+surg_CTT644E001_exportID_0180_pair_0540$1024x1280.JPG",
    "d4672b0f2c284ad7a1897cde5b99917a": "ref_CTT644E001_exportID_0194_pair_0572$1280x1024+surg_CTT644E001_exportID_0194_pair_0572$1280x1024.JPG",
    "c35218f6c79f4f90b2c4c2d4a9d9e034": "ref_CTT644E001_exportID_0178_pair_0533$1280x1024+surg_CTT644E001_exportID_0178_pair_0533$1024x1280.JPG",
    "f52300550c7c46b4bd6a7971e1135287": "ref_CTT644E001_exportID_0173_pair_0514$1280x1024+surg_CTT644E001_exportID_0173_pair_0514$1024x1280.JPG",
    "81a9a38ee6384bb1ba6e0223ec7c2faf": "ref_CTT644E001_exportID_0169_pair_0496$1280x1024+surg_CTT644E001_exportID_0169_pair_0496$1024x1280.JPG",
    "84384747867e48479a54c3de35c09f17": "ref_CTT644E001_exportID_0188_pair_0556$1280x1024+surg_CTT644E001_exportID_0188_pair_0556$1024x1280.JPG",
    "2743f867917b46f5ab903b4276212302": "ref_CTT644E001_exportID_0167_pair_0489$1280x1024+surg_CTT644E001_exportID_0167_pair_0489$1024x1280.JPG",
    "37baaaec967d4ba3b0b31c7f5e9a7394": "ref_CTT644E001_exportID_0180_pair_0539$1280x1024+surg_CTT644E001_exportID_0180_pair_0539$1024x1280.JPG",
    "94df02e8b1784161a6ce4054665ddc50": "ref_CTT644E001_exportID_0165_pair_0482$1280x1024+surg_CTT644E001_exportID_0165_pair_0482$1024x1280.JPG",
    "1039efee846f4dc29f0693858ddf1607": "ref_CTT644E001_exportID_0198_pair_0581$1280x1024+surg_CTT644E001_exportID_0198_pair_0581$1280x1024.JPG",
    "723b5e3b90084393802e52a95eecfd1e": "ref_CTT644E001_exportID_0172_pair_0508$1280x1024+surg_CTT644E001_exportID_0172_pair_0508$1024x1280.JPG",
    "5e6aab6e12584ed0b9790979445160cd": "ref_CTT644E001_exportID_0176_pair_0525$1280x1024+surg_CTT644E001_exportID_0176_pair_0525$1024x1280.JPG",
    "a411806fcabb446c9545e090ef5ad1ed": "ref_CTT644E001_exportID_0177_pair_0528$1280x1024+surg_CTT644E001_exportID_0177_pair_0528$1024x1280.JPG",
    "b548384af419494d81caf63c3cfd1900": "ref_CTT644E001_exportID_0175_pair_0520$1280x1024+surg_CTT644E001_exportID_0175_pair_0520$1024x1280.JPG",
    "9821c4c792944933ae31784a741699e0": "ref_CTT644E001_exportID_0192_pair_0565$1280x1024+surg_CTT644E001_exportID_0192_pair_0565$1280x1024.JPG",
    "c57e243ed6d446d19024405b71ca3746": "ref_CTT644E001_exportID_0151_pair_0431$1280x1024+surg_CTT644E001_exportID_0151_pair_0431$1024x1280.JPG",
    "e3e58cf9e765490b84ef04bb583b4149": "ref_CTT644E001_exportID_0148_pair_0421$1280x1024+surg_CTT644E001_exportID_0148_pair_0421$1024x1280.JPG",
    "2f63cf9dd4cd4df2979d4adf9d80e318": "ref_CTT644E001_exportID_0175_pair_0522$1280x1024+surg_CTT644E001_exportID_0175_pair_0522$1024x1280.JPG",
    "784f2815928042bb8180cb8e4c44f6a5": "ref_CTT644E001_exportID_0159_pair_0459$1280x1024+surg_CTT644E001_exportID_0159_pair_0459$1024x1280.JPG",
    "d0f304c169ae437b8c1d5ea552788d8f": "ref_CTT644E001_exportID_0169_pair_0495$1280x1024+surg_CTT644E001_exportID_0169_pair_0495$1024x1280.JPG",
    "11e1b1b98a674ac4a2d133484204cc6b": "ref_CTT644E001_exportID_0163_pair_0471$1280x1024+surg_CTT644E001_exportID_0163_pair_0471$1024x1280.JPG",
    "bd281ed4639542eaa1d1b1257603ce47": "ref_CTT644E001_exportID_0179_pair_0538$1280x1024+surg_CTT644E001_exportID_0179_pair_0538$1024x1280.JPG",
    "eb0f4b49b4824a21b21ae5fa93ca45cd": "ref_CTT644E001_exportID_0195_pair_0575$1280x1024+surg_CTT644E001_exportID_0195_pair_0575$1280x1024.JPG",
    "0033fc329d424a00aa24f6b345637460": "ref_CTT644E001_exportID_0192_pair_0563$1280x1024+surg_CTT644E001_exportID_0192_pair_0563$1280x1024.JPG",
    "d6973e17814a415aa111d7084b4bc8e0": "ref_CTT644E001_exportID_0166_pair_0485$1280x1024+surg_CTT644E001_exportID_0166_pair_0485$1024x1280.JPG",
    "6d2cd19fc2e8408f8200c66d274e3630": "ref_CTT644E001_exportID_0198_pair_0582$1280x1024+surg_CTT644E001_exportID_0198_pair_0582$1280x1024.JPG",
    "cf2737f728cd419cafc5ad9e3bf78760": "ref_CTT644E001_exportID_0156_pair_0454$1280x1024+surg_CTT644E001_exportID_0156_pair_0454$1024x1280.JPG",
    "816d40d2512d43d3882634c26ee0a3ee": "ref_CTT644E001_exportID_0150_pair_0430$1280x1024+surg_CTT644E001_exportID_0150_pair_0430$1024x1280.JPG",
    "7b02f49952f04708ba5361792e179606": "ref_CTT644E001_exportID_0193_pair_0568$1280x1024+surg_CTT644E001_exportID_0193_pair_0568$1280x1024.JPG",
    "1dd2e464fb694dd8934f1b93c5f71399": "ref_CTT644E001_exportID_0142_pair_0401$1280x1024+surg_CTT644E001_exportID_0142_pair_0401$1280x1024.JPG",
    "b6b5a30f44774e23ad1b5d04d5698f68": "ref_CTT644E001_exportID_0164_pair_0476$1280x1024+surg_CTT644E001_exportID_0164_pair_0476$1024x1280.JPG",
    "120d6c4453214ca1b4a7de7c135eb8da": "ref_CTT644E001_exportID_0174_pair_0515$1280x1024+surg_CTT644E001_exportID_0174_pair_0515$1024x1280.JPG",
    "18aafe42d1c14723bfaa04548d748755": "ref_CTT644E001_exportID_0195_pair_0577$1280x1024+surg_CTT644E001_exportID_0195_pair_0577$1280x1024.JPG",
    "30a8e937ccbf4aeb91830688b38d9950": "ref_CTT644E001_exportID_0143_pair_0406$1280x1024+surg_CTT644E001_exportID_0143_pair_0406$1280x1024.JPG",
    "701812341c4143c8a762b01f572edde4": "ref_CTT644E001_exportID_0172_pair_0510$1280x1024+surg_CTT644E001_exportID_0172_pair_0510$1024x1280.JPG",
    "f5907ef5d58e4bd48dff595e956f225a": "ref_CTT644E001_exportID_0177_pair_0530$1280x1024+surg_CTT644E001_exportID_0177_pair_0530$1024x1280.JPG",
    "429204f0b38b4b5fbd85d7c818d55c74": "ref_CTT644E001_exportID_0168_pair_0493$1280x1024+surg_CTT644E001_exportID_0168_pair_0493$1024x1280.JPG",
    "875f6768ad2e44a29163442e6640457b": "ref_CTT644E001_exportID_0198_pair_0579$1280x1024+surg_CTT644E001_exportID_0198_pair_0579$1280x1024.JPG",
    "d39fa2de57904652a1e4e035f0acb369": "ref_CTT644E001_exportID_0168_pair_0494$1280x1024+surg_CTT644E001_exportID_0168_pair_0494$1024x1280.JPG",
    "ee66e055cf624b569609655020c3408f": "ref_CTT644E001_exportID_0199_pair_0586$1280x1024+surg_CTT644E001_exportID_0199_pair_0586$1280x1024.JPG",
    "7e76ce0b0d934e6e97d6a9719e8c2068": "ref_CTT644E001_exportID_0168_pair_0491$1280x1024+surg_CTT644E001_exportID_0168_pair_0491$1024x1280.JPG",
    "2d49176105ef415997ae0f28602e39b3": "ref_CTT644E001_exportID_0169_pair_0497$1280x1024+surg_CTT644E001_exportID_0169_pair_0497$1024x1280.JPG",
    "917b6c59132043318c139ae7ccfcb41c": "ref_CTT644E001_exportID_0143_pair_0403$1280x1024+surg_CTT644E001_exportID_0143_pair_0403$1280x1024.JPG",
    "c68198ba626148109459eae14eeb70a5": "ref_CTT644E001_exportID_0192_pair_0566$1280x1024+surg_CTT644E001_exportID_0192_pair_0566$1280x1024.JPG",
    "ecbb7a46df134469b479eecd5dafcc83": "ref_CTT644E001_exportID_0193_pair_0567$1280x1024+surg_CTT644E001_exportID_0193_pair_0567$1280x1024.JPG",
    "e34de1340343497ba1cc790ad3158f7e": "ref_CTT644E001_exportID_0164_pair_0475$1280x1024+surg_CTT644E001_exportID_0164_pair_0475$1024x1280.JPG",
    "0119535231dc42bba3a84fcc57c470e6": "ref_CTT644E001_exportID_0162_pair_0468$1280x1024+surg_CTT644E001_exportID_0162_pair_0468$1024x1280.JPG",
    "0862bfc96a174e80b6d68fb16669c038": "ref_CTT644E001_exportID_0145_pair_0414$1280x1024+surg_CTT644E001_exportID_0145_pair_0414$1280x1024.JPG",
    "bf29522f98e24f18bda55ad60aac9c98": "ref_CTT644E001_exportID_0162_pair_0470$1280x1024+surg_CTT644E001_exportID_0162_pair_0470$1024x1280.JPG",
    "ebd0eeb3493e4532b4f4b6b0cc4bf381": "ref_CTT644E001_exportID_0203_pair_0591$1280x1024+surg_CTT644E001_exportID_0203_pair_0591$1280x1024.JPG",
    "0e3c8fbd1e5c4394a2fc6d5e2d8e940e": "ref_CTT644E001_exportID_0176_pair_0526$1280x1024+surg_CTT644E001_exportID_0176_pair_0526$1024x1280.JPG",
    "c937af0076b54d66ad6a6e9bf945410c": "ref_CTT644E001_exportID_0205_pair_0600$1280x1024+surg_CTT644E001_exportID_0205_pair_0600$1280x1024.JPG",
    "42c03675f6d943dc8ff349ad71cc13f8": "ref_CTT644E001_exportID_0205_pair_0599$1280x1024+surg_CTT644E001_exportID_0205_pair_0599$1280x1024.JPG",
    "425df7ebe7ad4649bae381aae5d11d9e": "ref_CTT644E001_exportID_0147_pair_0416$1280x1024+surg_CTT644E001_exportID_0147_pair_0416$1024x1280.JPG",
    "c155262ba6e74764b18ee65c7b10b988": "ref_CTT644E001_exportID_0199_pair_0585$1280x1024+surg_CTT644E001_exportID_0199_pair_0585$1280x1024.JPG",
    "7d3d44ec409849b38c29e97a3b50d457": "ref_CTT644E001_exportID_0159_pair_0461$1280x1024+surg_CTT644E001_exportID_0159_pair_0461$1024x1280.JPG",
    "d8353d269d454fe19968bfae6acc2eb0": "ref_CTT644E001_exportID_0189_pair_0561$1280x1024+surg_CTT644E001_exportID_0189_pair_0561$1280x1024.JPG",
    "79bda5926d7b48f38932e623e35feba7": "ref_CTT644E001_exportID_0185_pair_0547$1280x1024+surg_CTT644E001_exportID_0185_pair_0547$1024x1280.JPG",
    "ac3ff8320e43417db8b37f1cda7d618e": "ref_CTT644E001_exportID_0189_pair_0562$1280x1024+surg_CTT644E001_exportID_0189_pair_0562$1280x1024.JPG",
    "0c40e6d6408f4228901e13fe55d888bb": "ref_CTT644E001_exportID_0200_pair_0590$1280x1024+surg_CTT644E001_exportID_0200_pair_0590$1280x1024.JPG",
    "27882341da8b447abe4e8ca8706f4695": "ref_CTT644E001_exportID_0189_pair_0559$1280x1024+surg_CTT644E001_exportID_0189_pair_0559$1280x1024.JPG",
    "a7d3b3c5a7044549bf4de30926a6147e": "ref_CTT644E001_exportID_0175_pair_0519$1280x1024+surg_CTT644E001_exportID_0175_pair_0519$1024x1280.JPG",
    "37ca1c2a17cd4d96b4d6eea97401dd7a": "ref_CTT644E001_exportID_0148_pair_0422$1280x1024+surg_CTT644E001_exportID_0148_pair_0422$1024x1280.JPG",
    "9cad87183234434eaed10e93f6955522": "ref_CTT644E001_exportID_0172_pair_0509$1280x1024+surg_CTT644E001_exportID_0172_pair_0509$1024x1280.JPG",
    "37b8ae7bf03748e585152768790043d8": "ref_CTT644E001_exportID_0188_pair_0555$1280x1024+surg_CTT644E001_exportID_0188_pair_0555$1024x1280.JPG",
    "05d593b5ebc74ad6b646cb06b047cbfe": "ref_CTT644E001_exportID_0203_pair_0593$1280x1024+surg_CTT644E001_exportID_0203_pair_0593$1280x1024.JPG",
    "33dacaafa446478fa29a285d00651811": "ref_CTT644E001_exportID_0175_pair_0521$1280x1024+surg_CTT644E001_exportID_0175_pair_0521$1024x1280.JPG",
    "38288d4b08284b999bd1ddca336bde91": "ref_CTT644E001_exportID_0159_pair_0460$1280x1024+surg_CTT644E001_exportID_0159_pair_0460$1024x1280.JPG",
    "11e762d00aa24090b2d710e829b4081c": "ref_CTT644E001_exportID_0192_pair_0564$1280x1024+surg_CTT644E001_exportID_0192_pair_0564$1280x1024.JPG",
    "270302cef9d94fe484fab84681427494": "ref_CTT644E001_exportID_0176_pair_0524$1280x1024+surg_CTT644E001_exportID_0176_pair_0524$1024x1280.JPG",
    "82abeef4337f4d558ea8b3d7b593f382": "ref_CTT644E001_exportID_0168_pair_0492$1280x1024+surg_CTT644E001_exportID_0168_pair_0492$1024x1280.JPG",
    "6cb154a501db4c5b9abf7e0d6e2d27f8": "ref_CTT644E001_exportID_0178_pair_0534$1280x1024+surg_CTT644E001_exportID_0178_pair_0534$1024x1280.JPG",
    "d21b7e41cfc9449eb825376358b9faf4": "ref_CTT644E001_exportID_0170_pair_0499$1280x1024+surg_CTT644E001_exportID_0170_pair_0499$1024x1280.JPG",
    "a89295958bb34885abf3721282d7771e": "ref_CTT644E001_exportID_0181_pair_0543$1280x1024+surg_CTT644E001_exportID_0181_pair_0543$1024x1280.JPG",
    "1806dee4f8bc4c5c9979eb79dd60257a": "ref_CTT644E001_exportID_0149_pair_0424$1280x1024+surg_CTT644E001_exportID_0149_pair_0424$1024x1280.JPG",
    "1190bac482794f7f89bba8ff6d00ed37": "ref_CTT644E001_exportID_0195_pair_0578$1280x1024+surg_CTT644E001_exportID_0195_pair_0578$1280x1024.JPG",
    "4c619206aff845fb906bc619d6e00ab4": "ref_CTT644E001_exportID_0161_pair_0465$1280x1024+surg_CTT644E001_exportID_0161_pair_0465$1024x1280.JPG",
    "6018740f67e649d482d8ab89a8d1c2f6": "ref_CTT644E001_exportID_0185_pair_0550$1280x1024+surg_CTT644E001_exportID_0185_pair_0550$1024x1280.JPG",
    "db8a9ce9fdee441f8af3c995434b7a55": "ref_CTT644E001_exportID_0153_pair_0442$1280x1024+surg_CTT644E001_exportID_0153_pair_0442$1024x1280.JPG",
    "56522f35b56c49f48f8a5669255399eb": "ref_CTT644E001_exportID_0157_pair_0458$1280x1024+surg_CTT644E001_exportID_0157_pair_0458$1024x1280.JPG",
    "d619878ca9044829a62ea038a3b888c5": "ref_CTT644E001_exportID_0194_pair_0571$1280x1024+surg_CTT644E001_exportID_0194_pair_0571$1280x1024.JPG",
    "37febcfac7b3449f899b423afb0464f5": "ref_CTT644E001_exportID_0151_pair_0432$1280x1024+surg_CTT644E001_exportID_0151_pair_0432$1024x1280.JPG",
    "d465359897b04140a5c4e08e043b49ac": "ref_CTT644E001_exportID_0142_pair_0402$1280x1024+surg_CTT644E001_exportID_0142_pair_0402$1280x1024.JPG",
    "3514d04544844783af67f15f5f4ab70d": "ref_CTT644E001_exportID_0180_pair_0541$1280x1024+surg_CTT644E001_exportID_0180_pair_0541$1024x1280.JPG",
    "3aec7f0229024aa9aee37b9135f4749e": "ref_CTT644E001_exportID_0164_pair_0478$1280x1024+surg_CTT644E001_exportID_0164_pair_0478$1024x1280.JPG",
    "0eab34811475472eb11d79d4ab3fd032": "ref_CTT644E001_exportID_0162_pair_0469$1280x1024+surg_CTT644E001_exportID_0162_pair_0469$1024x1280.JPG",
    "02997c67862a4a8497500e4f137e304b": "ref_CTT644E001_exportID_0174_pair_0517$1280x1024+surg_CTT644E001_exportID_0174_pair_0517$1024x1280.JPG"
}
    for asset in json_export:
        logger.info(f"Asset: {asset_counter}")
        
        if asset["externalId"] in wrong_filesnames:
            external_id = wrong_filesnames[asset["externalId"]]
        else:
            external_id = asset["externalId"]
        
        # Get file extension

        file_extension = external_id.rsplit(".")[1]
    

        
        # get left and right external id before removing w and h
        left_external_id = external_id.split("+")[0]
        right_external_id = external_id.split("+")[1].split(".")[0]
        

        # filenames for output later
        left_output_filename = left_external_id.split("$")[0] + "." + file_extension
        right_output_filename = right_external_id.split("$")[0] + "." + file_extension

        # height and width of left and right images, and stitched image's total height and width
        left_width = int(left_external_id.split("$")[1].split("x")[0])
        left_height = int(left_external_id.split("$")[1].split("x")[1])
        right_width = int(right_external_id.split("$")[1].split(".")[0].split("x")[0])
        right_height = int(right_external_id.split("$")[1].split(".")[0].split("x")[1])
        try:
            total_width = asset["metadata"]["width"]
            total_height = asset["metadata"]["height"]
        except Exception as e:
            logger.info(f"{e} | {asset['assetId']}" )
            continue

        # calculate whitespace between images for conversions
        whitespace_between_images = int(total_width - right_width - left_width)

        left_ann = {}
        right_ann = {}

        # Go through annotations
        # toric_points = {
        #     "1": [1, 1],
        #     "2": [1, 1],
        #     "3": [1, 1],
        #     "4": [1, 1],
        #     "5": [1, 1],
        #     "6": [1, 1],
        # }
        for ann in asset["task"]["tools"]:
            # adjust polygon anns if necessary
            if "segmentation" in ann:
                for zone in ann["segmentation"]["zones"]:
                    polygon = zone["region"]
                    min_val = min(polygon, key=lambda x: x[0])[0]

                    # If the min value is greater than the left width,
                    # then it's on the right side
                    if min_val > left_width:  # Right side
                        for point in polygon:
                            right_min_x = left_width + whitespace_between_images
                            # if the point is less than the right min x
                            # it needs to be adjusted to the right min x
                            if point[0] < right_min_x:
                                point[0] = 0
                            else:
                                # adjust points as if left image and whitespace isnt there
                                point[0] -= right_min_x

                        # if the first and last points are not the same, append the first point to the end
                        if polygon[0] != polygon[-1]:
                            polygon.append(polygon[0])

                        right_ann[ann["title"]] = polygon
                    else:  # Left side
                        for point in polygon:
                            left_max_x = left_width
                            # if the point is greater than the left max x
                            # it needs to be adjusted to the left max x
                            if point[0] > left_max_x:
                                point[0] = left_max_x
                        # if the first and last points are not the same, append the first point to the end
                        if polygon[0] != polygon[-1]:
                            polygon.append(polygon[0])
                        left_ann[ann["title"]] = polygon
            elif ann.get("title") == "Keypoint_pairs":
                try:
                    answer = ann["classifications"][0]["answer"]
                except IndexError:
                    # Generate a UUID
                    generated_uuid = uuid.uuid4()

                    # Convert UUID to a hexadecimal string
                    uuid_hex = generated_uuid.hex

                    # Truncate to 4 digits
                    answer = uuid_hex[:4]
                if "Keypoint_pairs" not in left_ann:
                    left_ann["Keypoint_pairs"] = {}
                if "Keypoint_pairs" not in right_ann:
                    right_ann["Keypoint_pairs"] = {}
                if ann.get("point")[0] > left_width:  # right side
                    # first change x coord with out left stiched image
                    ann['point'][0] = (ann['point'][0] - (left_width + whitespace_between_images))
                    right_ann[ann["title"]][answer] = ann["point"]
                elif ann.get("point")[0] < left_width:
                    left_ann[ann["title"]][answer] = ann["point"]
            # elif ann.get("title") == "Toric_dots":
            #     answer = ann["classifications"][0]["answer"]
            #     # if "Toric_dots" not in left_ann:
            #     #     left_ann["Toric_dots"] = {}
            #     if "Toric_dots" not in right_ann:
            #         right_ann["Toric_dots"] = {}
            #     toric_points[answer] = ann["point"]

        # for key, value in toric_points.items():
        #     right_ann["Toric_dots"][key] = value

        with open(
            f"{output_folder}/{left_output_filename.split('.')[0]}.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(left_ann, f)
        with open(
            f"{output_folder}/{right_output_filename.split('.')[0]}.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(right_ann, f)
            
        asset_counter += 1

    logger.info("script completed. zipping output")
    return output_folder

if "name" == "__main__":
    alcon_conversions()
