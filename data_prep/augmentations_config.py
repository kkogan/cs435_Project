import albumentations as A

from typing import Dict
from pyspark.sql import Row

# TODO: Consider these augmentations https://demo.albumentations.ai/
    # cropping/dropout/blur
    #     CoarseDropout  (gray)?
    #     CenterCrop # RandomSizedCrop?
    #     MotionBlur
    # for time of day
    #     ColorJitter ?
    #     RandomGamma ?
    # Weather
    #   Spatter rain/mud
    #   RandomSunFlare
    #   RandomShadow
STANDARD_AUGMENTATIONS = {
    "spatter-mud": A.Spatter(always_apply=False, p=1.0, mean=(0.65, 0.65), std=(0.3, 0.3), gauss_sigma=(2.0, 2.0), intensity=(0.6, 0.6), cutout_threshold=(0.68, 0.68), mode=['mud']),
    "spatter-rain": A.Spatter(always_apply=False, p=1.0, mean=(0.65, 0.65), std=(0.3, 0.3), gauss_sigma=(2.25, 2.75), intensity=(0.6, 0.6), cutout_threshold=(0.68, 0.68), mode=['rain'])
}

def augmentations_for_row(row:Row) -> Dict:
    """different decision per class"""
    class_name = row.class_name
    species = row.species    
    
    # On second thought, augment all for now
    if 'Apple' == species:
        if 'Apple___healthy' == class_name:
            #return if_is_unaugmented(row, 1.0)
            return STANDARD_AUGMENTATIONS 
        if 'Apple___Apple_scab' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Apple___Black_rot' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Apple___Cedar_apple_rust' == class_name:
            return STANDARD_AUGMENTATIONS
        else:
            return STANDARD_AUGMENTATIONS
        
    elif 'Cherry_(including_sour)' == species:
        if 'Cherry_(including_sour)___healthy' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Cherry_(including_sour)___Powdery_mildew' == class_name:
            return STANDARD_AUGMENTATIONS
        else:
            return STANDARD_AUGMENTATIONS
        
    elif 'Corn_(maize)' == species:
        if 'Corn_(maize)___healthy' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Corn_(maize)___Northern_Leaf_Blight' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Corn_(maize)___Common_rust_' == class_name:
            return STANDARD_AUGMENTATIONS
        else:
            return STANDARD_AUGMENTATIONS
        
    elif 'Grape' == species:
        if 'Grape___healthy' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Grape___Esca_(Black_Measles)' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Grape___Black_rot' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Grape___Leaf_blight_(Isariopsis_Leaf_Spot)' == class_name:
            return STANDARD_AUGMENTATIONS
        else:
            return STANDARD_AUGMENTATIONS
        
    elif 'Peach' == species:
        if 'Peach___healthy' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Peach___Bacterial_spot' == class_name:
            return STANDARD_AUGMENTATIONS
        else:
            return STANDARD_AUGMENTATIONS
        
    elif 'Pepper,_bell' == species:
        if 'Pepper,_bell___healthy' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Pepper,_bell___Bacterial_spot' == class_name:
            return STANDARD_AUGMENTATIONS
        else:
            return STANDARD_AUGMENTATIONS
        
    elif 'Potato' == species:
        if 'Potato___healthy' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Potato___Early_blight' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Potato___Late_blight' == class_name:
            return STANDARD_AUGMENTATIONS
        else:
            return STANDARD_AUGMENTATIONS
        
    elif 'Strawberry' == species:
        if 'Strawberry___healthy' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Strawberry___Leaf_scorch' == class_name:
            return STANDARD_AUGMENTATIONS
        else:
            return STANDARD_AUGMENTATIONS
        
    elif 'Tomato' == species:
        if 'Tomato___healthy' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Tomato___Early_blight' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Tomato___Leaf_Mold' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Tomato___Late_blight' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Tomato___Target_Spot' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Tomato___Tomato_mosaic_virus' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Tomato___Septoria_leaf_spot' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Tomato___Bacterial_spot' == class_name:
            return STANDARD_AUGMENTATIONS
        if 'Tomato___Tomato_Yellow_Leaf_Curl_Virus' == class_name:
            return STANDARD_AUGMENTATIONS
        else:
            return STANDARD_AUGMENTATIONS
    #default    
    return None


# def if_is_unaugmented(row, proba):
#     return proba if row.augmentation is None else 0


# def is_unaugmented_or_some_augmentions(row, augmentations, proba):
#     return proba if row.augmentation is None or row.augmentation in augmentations else 0
