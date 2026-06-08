import os

YOLO_MODEL_PATH = os.getenv("YOLO_MODEL_PATH", "/project/models/anime_face_detection.pt")
FACE_CONF_THRESHOLD = float(os.getenv("FACE_CONF_THRESHOLD", "0.3"))
FACE_MIN_PX = 30
ASPECT_RATIO_MIN = 0.4
ASPECT_RATIO_MAX = 2.5
