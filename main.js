contents = [];
elements = [];

function preload() {
    prepare(layerN, layerSize);
}

function setup() {
    seed = int(fxrand() * 100000000);
    randomSeed(seed);
    createCanvas(windowWidth, windowHeight);
    for (var i = 0; i < this.layerN; i++) {
        elements[i] = new Layer(contents[i]);
    }
}

function draw() {
    background(bgColor[0],bgColor[1],bgColor[2]);
    for (var i = 0; i < this.layerN; i++) {
        elements[i].display();
    }
}

function windowResized() {
    resizeCanvas(windowWidth, windowHeight);
}