package org.atlasapi.content;

public class Player extends Described {

    public static Player copyTo(Player from, Player to) {
        Described.copyTo(from, to);
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Player) {
            copyTo(this, (Player) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public Player copy() {
        return copyTo(this, new Player());
    }

    @Override
    public Described createNew() {
        return new Player();
    }

}
