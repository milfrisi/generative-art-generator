function Layer(contents = []) {
  this.contents = contents;
  this.length = this.contents.length;

  var r = floor(random(0, this.length));
  print(r);

  this.display = function () {
    imageMode(CENTER);
    image(this.contents[r], windowWidth / 2, windowHeight / 2);
    if (windowWidth > windowHeight) {
      this.contents[r].resize(
        0,
        windowHeight - (windowHeight * borderArea) / 100
      );
    } else if (windowWidth == windowHeight) {
      if (imageOrientation == "landscape") {
        this.contents[r].resize(
          windowWidth - (windowWidth * borderArea) / 100,
          0
        );
      }
      if (imageOrientation == "portrait") {
        this.contents[r].resize(
          0,windowHeight - (windowHeight * borderArea) / 100
        );
      }
    } else {
      this.contents[r].resize(
        windowWidth - (windowWidth * borderArea) / 100,
        0
      );
    }
  };
}
