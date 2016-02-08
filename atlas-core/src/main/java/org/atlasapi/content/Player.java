package org.atlasapi.content;

public class Player extends Described {

    @Override
    public Player copy() {
        Player player = new Player();
        copyTo(this, player);
        return player;
    }

}
