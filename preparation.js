function validateInput(layerN, layerSize = []) {
    this.layerN = layerN;
    this.layerSize = layerSize;
  
    if (layerN != layerSize.length) {
      return false;
    } else {
      console.log("Alles klar!");
      return true;
    }
  }
  
  function prepare(layerN, layerSize = []) {
    this.layerN = layerN;
    this.layerSize = layerSize;
  
    if (validateInput(this.layerN, this.layerSize)) {
  
      for (var i = 0; i < this.layerN; i++) { // iterate LayerN
        contents[i] = [];
        for (var j = 0; j < this.layerSize[i]; j++) {
          contents[i][j] = loadImage(i + "_" + j + ".png"); // i=0, j=0, contents[0] = [layer00_0, layer00_1, layer00_2]
        }
      }
  
    } else {
      // throw new Error("Teliti lagi!");
    }
  }