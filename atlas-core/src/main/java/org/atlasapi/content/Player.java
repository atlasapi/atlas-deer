package org.atlasapi.content;

public class Player extends Described {

    @Override
    public Described copy() {
        Player player = new Player();
        copyTo(this, player);
        return player;
    }

}
