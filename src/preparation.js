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
  
      for (var i = 0; i < this.layerN; i++) { // iterate the number of layers
        contents[i] = [];
        for (var j = 0; j < this.layerSize[i]; j++) {
          contents[i][j] = loadImage(`assets/${i}_${j}.png`);
        }
      }
  
    } else {
      console.log("Make sure the file names and numbers are correct");
    }
  }