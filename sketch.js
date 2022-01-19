/*
  Developed by MOOD.

  If you think this project is useful and would like to say thanks, 
  you can contribute to this Tezos wallet:
    tz1LVRhCSdGHSL1V7EtqjgoucKPi2hJ1DLnu

  # HOW TO START
  
  Rename your files with this format:
  
  (elements)_(variants).png
  
  e.g.:
  
  1st element has 4 variants
  2nd element has 3 variants
  3rd element has 2 variants
  
  Your filenames will be:
  
  0_0.png
  0_1.png
  0_2.png
  0_3.png
  1_0.png
  1_1.png
  1_2.png
  2_0.png
  2_1.png
  
  Yes, we count from zero
  And your variables will be:
  
  layerN = 3;
  layerSize = [4,3,2];
  
  please change these variables below:
  
*/

// Define number of elements
layerN = 4;

// Define variants for each elements
layerSize = [3,3,3,3];

// Define image orientation ("portrait" or "landscape") for display responsiveness
imageOrientation = "portrait";

// Define background color in RGB
bgColor =[128,128,128]

// Define border area percentage
borderArea = 0;
