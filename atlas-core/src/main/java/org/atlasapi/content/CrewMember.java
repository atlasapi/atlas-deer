package org.atlasapi.content;

import java.util.Map;
import java.util.Set;

import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.base.Maybe;

public class CrewMember extends Identified {

    public static final String TVA_URI_2001_PREFIX = "urn:mpeg:mpeg7:cs:RoleCS:2001";
    public static final String TVA_URI_2010_PREFIX = "urn:tva:metadata:cs:TVARoleCS:2010";
    private final static String EMPTY = "";

    public enum Role {
        ABRIDGED_BY("abridged_by", "Abridged By", EMPTY, EMPTY),
        ART_DIRECTOR("art_director", "Art Director", "ART-DIRECTOR", TVA_URI_2001_PREFIX),
        SUPERVISING_DIRECTOR("supervising_director", "Supervising Director", EMPTY, EMPTY),
        SOURCE_WRITER("source_writer", "Source Writer", EMPTY, EMPTY),
        ASSISTANT_DIRECTOR("assistant_director", "Assistant Director", "ASSISTANT-DIRECTOR", TVA_URI_2001_PREFIX),
        ASSOCIATE_DIRECTOR("associate_director", "Associate Director", EMPTY, EMPTY),
        ADDITIONAL_DIRECTOR("additional_director", "Additional Director", EMPTY, EMPTY),
        COLLABORATING_DIRECTOR("collaborating_director", "Collaborating Director", EMPTY, EMPTY),
        CO_DIRECTOR("co-director", "Co-Director", EMPTY, EMPTY),
        DEPUTY_EDITOR("deputy_editor", "Deputy Editor", EMPTY, EMPTY),
        DIRECTOR("director", "Director", "DIRECTOR", TVA_URI_2001_PREFIX),
        DRAMATISED_BY("dramatised_by", "Dramatised By", EMPTY, EMPTY),
        EDITOR("editor", "Editor", EMPTY, EMPTY),
        EXECUTIVE_EDITOR("executive_editor", "Executive Editor", EMPTY, EMPTY),
        EXECUTIVE_PRODUCER("executive_producer", "Executive Producer", "EXECUTIVE-PRODUCER", TVA_URI_2001_PREFIX),
        PRODUCER("producer", "Producer", "PRODUCER", TVA_URI_2001_PREFIX),
        SERIES_DIRECTOR("series_director", "Series Director", EMPTY, EMPTY),
        SERIES_EDITOR("series_editor", "Series Editor", "AD12", TVA_URI_2010_PREFIX),
        SERIES_PRODUCER("series_producer", "Series Producer", "AD13", TVA_URI_2010_PREFIX),
        WRITER("writer", "Writer", EMPTY, EMPTY),
        ADAPTED_BY("adapted_by", "Adapted By", EMPTY, EMPTY),
        PRESENTER("presenter", "Presenter", "AD6", TVA_URI_2010_PREFIX),
        COMPOSER("composer", "Composer", "COMPOSER", TVA_URI_2001_PREFIX),
        ACTOR("actor", "Actor", "ACTOR", TVA_URI_2001_PREFIX),
        PARTICIPANT("participant", "Participant", "V43", TVA_URI_2010_PREFIX),
        NARRATOR("narrator", "Narrator", "NARRATOR", TVA_URI_2001_PREFIX),
        REPORTER("reporter", "Reporter", "REPORTER", TVA_URI_2001_PREFIX),
        COMMENTATOR("commentator", "Commentator", EMPTY, EMPTY),
        EXPERT("expert", "Expert", "V96", TVA_URI_2010_PREFIX),
        ARTIST("artist", "Artist", EMPTY, EMPTY),
        ADVERTISING_AGENCY("advertising_agency", "Advertising Agency", "AD1", TVA_URI_2010_PREFIX),
        ADVERTISING_PRODUCTION_COMPANY("advertising_production_company", "Advertising Production Company", "AD2", TVA_URI_2010_PREFIX),
        ADVERTISER("advertiser", "Advertiser", "AD3", TVA_URI_2010_PREFIX),
        COMMISSIONING_CHANNEL("commissioning_channel", "Commissioning Channel", "AD4", TVA_URI_2010_PREFIX),
        COMMISSIONING_BRAND("commissioning_brand", "Commissioning Brand", "AD5", TVA_URI_2010_PREFIX),
        STUDIO_MANAGER("studio_manager", "Studio Manager", "AD7", TVA_URI_2010_PREFIX),
        ASSISTANT_STUDIO_MANAGER("assistant_studio_manager", "Assistant Studio Manager", "AD8", TVA_URI_2010_PREFIX),
        CAPTION_AUTHOR("caption_author", "Caption Author", "AD9", TVA_URI_2010_PREFIX),
        ELECTRICIAN("electrician", "Electrician", "AD10", TVA_URI_2010_PREFIX),
        JUDGE("judge", "Judge", "AD11", TVA_URI_2010_PREFIX),
        SCENARIO("scenario", "Scenario", "V2", TVA_URI_2010_PREFIX),
        COMMISSIONING_BROADCASTER("commissioning_broadcaster", "Commissioning Broadcaster", "V19", TVA_URI_2010_PREFIX),
        PRODUCTION_COMPANY("production_company", "Production Company", "V20", TVA_URI_2010_PREFIX),
        PRODUCTION_DEPARTMENT("production_department", "Production Department", "V22", TVA_URI_2010_PREFIX),
        EDITOR_IN_CHIEF("editor_in_chief", "Editor-in-chief", "V30", TVA_URI_2010_PREFIX),
        EDITOR_OF_THE_DAY("editor_of_the_day", "Editor-of-the-Day", "V31", TVA_URI_2010_PREFIX),
        COMMENTARY_OR_COMMENTATOR("commentary_or_commentator", "Commentary or Commentator", "V32", TVA_URI_2010_PREFIX),
        CONDUCTOR("conductor", "Conductor", "V42", TVA_URI_2010_PREFIX),
        ILLUSTRATOR("illustrator", "Illustrator", "V44", TVA_URI_2010_PREFIX),
        PHOTOGRAPHER("photographer", "Photographer", "V45", TVA_URI_2010_PREFIX),
        SOUND_RECORDIST("sound_recordist", "Sound Recordist", "V49", TVA_URI_2010_PREFIX),
        MANUFACTURER("manufacturer", "Manufacturer", "V55", TVA_URI_2010_PREFIX),
        ADAPTOR("adaptor", "Adaptor", "V76", TVA_URI_2010_PREFIX),
        SET_DRESSER("set_dresser", "Set Dresser", "V77", TVA_URI_2010_PREFIX),
        CONSULTANT("consultant", "Consultant", "V79", TVA_URI_2010_PREFIX),
        CHOREOGRAPHER("choreographer", "Choreographer", "V80", TVA_URI_2010_PREFIX),
        VISUAL_EDITOR("visual_editor", "Visual Editor", "V82", TVA_URI_2010_PREFIX),
        DIRECTOR_OF_PHOTOGRAPHY("director_of_photography", "Director of photography", "V83", TVA_URI_2010_PREFIX),
        ORCHESTRA("orchestra", "Orchestra", "V88", TVA_URI_2010_PREFIX),
        PROGRAMME_PROPOSAL("programme_proposal", "Treatment/Programme Proposal", "V94", TVA_URI_2010_PREFIX),
        TRANSLATION("translation", "Translation", "V95", TVA_URI_2010_PREFIX),
        INTERVIEWED_GUEST("interviewed_guest", "Interviewed Guest", "V97", TVA_URI_2010_PREFIX),
        ANNOUNCER("announcer", "Announcer", "V103", TVA_URI_2010_PREFIX),
        SPECIAL_EFFECTS("special_effects", "Special Effects", "V105", TVA_URI_2010_PREFIX),
        KEY_TALENT("key_talent", "Key talent", "V106", TVA_URI_2010_PREFIX),
        CASTING("casting", "Casting", "V110", TVA_URI_2010_PREFIX),
        WITNESS("witness", "Witness", "V117", TVA_URI_2010_PREFIX),
        CORRESPONDENT("correspondent", "Correspondent", "V483", TVA_URI_2010_PREFIX),
        COSTUME_DESIGNER("costume_designer", "Costume designer", "V484", TVA_URI_2010_PREFIX),
        DRESSER("dresser", "Dresser", "V485", TVA_URI_2010_PREFIX),
        NEWS_EDITOR("news_editor", "Editor/Producer (News)", "V486", TVA_URI_2010_PREFIX),
        FLOOR_MANAGER("floor_manager", "Floor Manager", "V487", TVA_URI_2010_PREFIX),
        GRAPHIC_ASSISTANT("graphic_assistant", "Graphic Assistant", "V488", TVA_URI_2010_PREFIX),
        GRAPHIC_DESIGNER("graphic_designer", "Graphic Designer", "V489", TVA_URI_2010_PREFIX),
        POST_PRODUCTION_EDITOR("post_production_editor", "Post-Production editor", "V490", TVA_URI_2010_PREFIX),
        PRODUCTION_MANAGER("production_manager", "Production Manager", "V491", TVA_URI_2010_PREFIX),
        PRODUCTION_SECRETARY("production_secretary", "Production Secretary", "V492", TVA_URI_2010_PREFIX),
        PROGRAMME_PRODUCTION_RESEARCHER("programme_production_researcher", "Programme Production Researcher", "V493", TVA_URI_2010_PREFIX),
        RIGGER("rigger", "Rigger", "V494", TVA_URI_2010_PREFIX),
        RUNNER("runner", "Runner", "V495", TVA_URI_2010_PREFIX),
        SCENIC_OPERATIVE("scenic_operative", "Scenic Operative", "V496", TVA_URI_2010_PREFIX),
        ASSISTANT_PRODUCER("assistant_producer", "Assistant Producer", "V497", TVA_URI_2010_PREFIX),
        BROADCAST_ASSISTANT("broadcast_assistant", "Broadcast Assistant", "V498", TVA_URI_2010_PREFIX),
        DUBBER("dubber", "Dubber", "V708", TVA_URI_2010_PREFIX),
        KEY_CHARACTER("key_character", "Key character", "V709", TVA_URI_2010_PREFIX),
        STUNTS("stunts", "Stunts", "V710", TVA_URI_2010_PREFIX),
        FIGHT_DIRECTOR("fight_director", "Fight Director", "V714", TVA_URI_2010_PREFIX),
        SCRIPT_SUPERVISOR("script_supervisor", "Script Supervisor", "V715", TVA_URI_2010_PREFIX),
        SECOND_ASSISTANT_DIRECTOR("second_assistant_director", "Second Assistant Director", "V716", TVA_URI_2010_PREFIX),
        SECOND_UNIT_DIRECTOR("second_unit_director", "Second Unit Director", "V717", TVA_URI_2010_PREFIX),
        SOUND_DESIGNER("sound_designer", "Sound Designer", "V718", TVA_URI_2010_PREFIX),
        MUSIC_ARRANGER("music_arranger", "Music Arranger", "V719", TVA_URI_2010_PREFIX),
        CAUSEUR("causeur", "Causeur", "V720", TVA_URI_2010_PREFIX),
        NEWS_READER("news_reader", "News Reader", "V721", TVA_URI_2010_PREFIX),
        ASSISTANT_CHIEF_LIGHTING_TECHNICIAN("assistant_chief_lighting_technician", "Assistant Chief Lighting Technician", "V724", TVA_URI_2010_PREFIX),
        CARPENTER("carpenter", "Carpenter", "V725", TVA_URI_2010_PREFIX),
        DIALOGUE_COACH("dialogue_coach", "Dialogue Coach", "V727", TVA_URI_2010_PREFIX),
        DRAUGHTSMAN("draughtsman", "Draughtsman", "V728", TVA_URI_2010_PREFIX),
        HAIRDRESSER("hairdresser", "Hairdresser", "V729", TVA_URI_2010_PREFIX),
        LEADMAN("leadman", "Leadman", "V730", TVA_URI_2010_PREFIX),
        ASSISTANT_VISUAL_EDITOR("assistant_visual_editor", "Assistant Visual Editor", "V734", TVA_URI_2010_PREFIX),
        CLAPPER_LOADER("clapper_loader", "Clapper Loader", "V735", TVA_URI_2010_PREFIX),
        FOCUS_PULLER("focus_puller", "Focus Puller", "V736", TVA_URI_2010_PREFIX),
        FOLEY_ARTIST("foley_artist", "Foley Artist", "V737", TVA_URI_2010_PREFIX),
        FOLEY_EDITOR("foley_editor", "Foley Editor", "V738", TVA_URI_2010_PREFIX),
        FOLEY_MIXER("foley_mixer", "Foley Mixer", "V739", TVA_URI_2010_PREFIX),
        GRIP("grip", "Grip", "V740", TVA_URI_2010_PREFIX),
        KEY_GRIP("key_grip", "Key Grip", "V741", TVA_URI_2010_PREFIX),
        MATTE_ARTIST("matte_artist", "Matte Artist", "V742", TVA_URI_2010_PREFIX),
        PYROTECHNICIAN("pyrotechnician", "Pyrotechnician", "V743", TVA_URI_2010_PREFIX),
        SECOND_ASSISTANT_CAMERA("second_assistant_camera", "Second Assistant Camera", "V744", TVA_URI_2010_PREFIX),
        SOUND_MIXER("sound_mixer", "Sound Mixer", "V745", TVA_URI_2010_PREFIX),
        VISION_MIXER("vision_mixer", "Vision mixer", "V746", TVA_URI_2010_PREFIX),
        ANIMAL_TRAINER("animal_trainer", "Animal Trainer", "V748", TVA_URI_2010_PREFIX),
        ARMOURER("armourer", "Armourer", "V749", TVA_URI_2010_PREFIX),
        GREENSMAN("greensman", "Greensman", "V750", TVA_URI_2010_PREFIX),
        LOCATION_MANAGER("location_manager", "Location Manager", "V751", TVA_URI_2010_PREFIX),
        SIGN_LANGUAGE("sign_language", "Sign Language", "V753", TVA_URI_2010_PREFIX),
        SUBTITLES("subtitles", "Subtitles", "V754", TVA_URI_2010_PREFIX),
        TRANSPORTATION_MANAGER("transportation_manager", "Transportation Manager", "V755", TVA_URI_2010_PREFIX),
        CHOIR("choir", "Choir", "V807", TVA_URI_2010_PREFIX),
        ENSEMBLE("ensemble", "Ensemble", "V808", TVA_URI_2010_PREFIX),
        MUSIC_GROUP("music_group", "Music Group", "V809", TVA_URI_2010_PREFIX),
        LIBRETTIST("librettist", "Librettist", "V810", TVA_URI_2010_PREFIX),
        LYRICIST("lyricist", "Lyricist", "V811", TVA_URI_2010_PREFIX),
        COMPUTER_PROGRAMMER("computer_programmer", "Computer programmer", "V812", TVA_URI_2010_PREFIX),
        PUPPETEER("puppeteer", "Puppeteer", "V813", TVA_URI_2010_PREFIX),
        AGGREGATOR("aggregator", "Aggregator", "AGGREGATOR", TVA_URI_2001_PREFIX),
        ANCHOR("anchor", "Anchor", "ANCHOR", TVA_URI_2001_PREFIX),
        ANIMATOR("animator", "Animator", "ANIMATOR", TVA_URI_2001_PREFIX),
        AUTHOR("author", "Author", "AUTHOR", TVA_URI_2001_PREFIX),
        BROADCASTER("broadcaster", "Broadcaster", "BROADCASTER", TVA_URI_2001_PREFIX),
        CAMERA_ASSISTANT("camera_assistant", "Camera Assistant", "CAMERA-ASSISTANT", TVA_URI_2001_PREFIX),
        CAMERA_OPERATOR("camera_operator", "Camera Operator", "CAMERA-OPERATOR", TVA_URI_2001_PREFIX),
        COMPUTER_GRAPHICS_ARTIST("computer_graphics_artist", "Computer Graphics Artist", "CG-ARTIST", TVA_URI_2001_PREFIX),
        CONTINUITY_PERSON("continuity_person", "Continuity Person", "CONTINUITY-PERSON", TVA_URI_2001_PREFIX),
        COSTUMER("costumer", "Costumer", "COSTUMER", TVA_URI_2001_PREFIX),
        COSTUME_SUPERVISOR("costume_supervisor", "Costume Supervisor", "COSTUME-SUPERVISOR", TVA_URI_2001_PREFIX),
        DANCER("dancer", "Dancer", "DANCER", TVA_URI_2001_PREFIX),
        DISSEMINATOR("disseminator", "Disseminator", "DISSEMINATOR", TVA_URI_2001_PREFIX),
        DISTRIBUTOR("distributor", "Distributor", "DISTRIBUTOR", TVA_URI_2001_PREFIX),
        INTERVIEWER("interviewer", "Interviewer", "INTERVIEWER", TVA_URI_2001_PREFIX),
        LIGHTING_OPERATOR("lighting_operator", "Lighting Operator", "LIGHTING-OPERATOR", TVA_URI_2001_PREFIX),
        LIGHTING_SUPERVISOR("lighting_supervisor", "Lighting Supervisor", "LIGHTING-SUPERVISOR", TVA_URI_2001_PREFIX),
        MAKEUP_ARTIST("makeup_artist", "Makeup Artist", "MAKEUP-ARTIST", TVA_URI_2001_PREFIX),
        MAKEUP_SUPERVISOR("makeup_supervisor", "Makeup Supervisor", "MAKEUP-SUPERVISOR", TVA_URI_2001_PREFIX),
        MUSICIAN("musician", "Musician", "MUSICIAN", TVA_URI_2001_PREFIX),
        MUSIC_SUPERVISOR("music_supervisor", "Music Supervisor", "MUSIC-SUPERVISOR", TVA_URI_2001_PREFIX),
        PERFORMER("performer", "Performer", "PERFORMER", TVA_URI_2001_PREFIX),
        PRODUCTION_ASSISTANT("production_assistant", "Production Assistant", "PRODUCTION-ASSISTANT", TVA_URI_2001_PREFIX),
        PRODUCTION_DESIGNER("production_designer", "Production Designer", "PRODUCTION-DESIGNER", TVA_URI_2001_PREFIX),
        PROPERTY_ASSISTANT("property_assistant", "Property Assistant", "PROPERTY-ASSISTANT", TVA_URI_2001_PREFIX),
        PROPERTY_MASTER("property_master", "Property Master", "PROPERTY-MASTER", TVA_URI_2001_PREFIX),
        PUBLISHER("publisher", "Publisher", "PUBLISHER", TVA_URI_2001_PREFIX),
        SCRIPTWRITER("scriptwriter", "Scriptwriter", "SCRIPTWRITER", TVA_URI_2001_PREFIX),
        SET_DESIGNER("set_designer", "Set Designer", "SET-DESIGNER", TVA_URI_2001_PREFIX),
        SET_MAKER("set_maker", "Set Maker", "SET-MAKER", TVA_URI_2001_PREFIX),
        SPECIAL_EFFECTS_ASSISTANT("special_effects_assistant", "Special Effects Assistant", "SFX-ASSISTANT", TVA_URI_2001_PREFIX),
        SPECIAL_EFFECTS_SUPERVISOR("special_effects_supervisor", "Special Effects Supervisor", "SFX-SUPERVISOR", TVA_URI_2001_PREFIX),
        SINGER("singer", "Singer", "SINGER", TVA_URI_2001_PREFIX),
        SOUND_EFFECTS_PERSON("sound_effects_person", "Sound Effects Person", "SOUND-EFFECTS-PERSON", TVA_URI_2001_PREFIX),
        SOUND_ENGINEER("sound_engineer", "Sound Engineer", "SOUND-ENGINEER", TVA_URI_2001_PREFIX),
        SOUND_SUPERVISOR("sound_supervisor", "Sound Supervisor", "SOUND-SUPERVISOR", TVA_URI_2001_PREFIX),
        STAFF("staff", "Staff", "STAFF", TVA_URI_2001_PREFIX),
        SWITCHER("switcher", "Switcher", "SWITCHER", TVA_URI_2001_PREFIX),
        SYNDICATOR("syndicator", "Syndicator", "SYNDICATOR", TVA_URI_2001_PREFIX),
        TECHNICAL_DIRECTOR("technical_director", "Technical Director", "TECHNICAL-DIRECTOR", TVA_URI_2001_PREFIX),
        TIMEKEEPER("timekeeper", "Timekeeper", "TIMEKEEPER", TVA_URI_2001_PREFIX),
        TRANSPORTATION_CAPTAIN("transportation_captain", "Transportation Captain", "TRANSPORTATION-CAPTAIN", TVA_URI_2001_PREFIX),
        UNKNOWN("unknown", "Unknown", "UNKNOWN", TVA_URI_2001_PREFIX),
        VIDEO_ENGINEER("video_engineer", "Video Engineer", "VIDEO-ENGINEER", TVA_URI_2001_PREFIX),
        VISUAL_EFFECTS_SUPERVISOR("visual_effects_supervisor", "Visual Effects Supervisor", "VISUAL-EFFECTS-SUPERVISOR", TVA_URI_2001_PREFIX),
        WEBCASTER("webcaster", "Webcaster", "WEBCASTER", TVA_URI_2001_PREFIX),
        ;
        
        private final String key;
        private final String title;
        private final String  tvaCode;
        private final String tvaUriPrefix;
        
        Role(String key, String title) {
            this.key = key;
            this.title = title;
            this.tvaCode = null;
            this.tvaUriPrefix = null;
        }

        Role(String key, String title, String tvaCode, String tvaUriPrefix) {
            this.key = key;
            this.title = title;
            this.tvaCode = tvaCode;
            this.tvaUriPrefix = tvaUriPrefix;
        }
        
        @FieldName("key")
        public String key() {
            return key;
        }
        
        @FieldName("title")
        public String title() {
            return title;
        }
        
        public static Role fromKey(String key) {
            Maybe<Role> role = fromPossibleKey(key);
            if (role.isNothing()) {
            	throw new IllegalArgumentException("Unknown role: "+key);
            }
            return role.requireValue();
        }

        public static Maybe<Role> fromPossibleKey(String key) {
            Maybe<Role> possibleRole = roleKeyMap.get(key);
            return possibleRole != null ? possibleRole : Maybe.<Role>nothing();
        }
        
		private static Map<String,Maybe<Role>> roleKeyMap = initRoleKeyMap();

        private static Map<String, Maybe<Role>> initRoleKeyMap() {
            ImmutableMap.Builder<String, Maybe<Role>> builder = ImmutableMap.builder();
            for (Role role : Role.values()) {
                builder.put(role.key(), Maybe.just(role));
            }
            return builder.build();
        }
    }
    
    private Role role;
    protected String name;
    private Publisher publisher;
    
    public CrewMember() {
        super();
    }
    
    public CrewMember(String uri, String curie, Publisher publisher) {
        super(uri, curie);
        this.publisher = publisher;
    }
    
    @FieldName("role")
    public Role role() {
        return role;
    }
    
    @FieldName("name")
    public String name() {
        return name;
    }
    
    @FieldName("publisher")
    public Publisher publisher() {
        return publisher;
    }
    
    @FieldName("profile_links")
    public Set<String> profileLinks() {
        return getAliasUrls();
    }
    
    public CrewMember withPublisher(Publisher publisher) {
        this.publisher = publisher;
        return this;
    }
    
    public CrewMember withRole(Role role) {
        this.role = role;
        return this;
    }
    
    public CrewMember withName(String name) {
        this.name = name;
        return this;
    }
    
    public CrewMember withProfileLink(String profileLink) {
        this.addAliasUrl(profileLink);
        return this;
    }
    
    public CrewMember withProfileLinks(Set<String> profileLinks) {
        this.setAliasUrls(profileLinks);
        return this;
    }
    
    public static CrewMember crewMemberWithoutId(String name, String roleKey, Publisher publisher) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Name is null or blank");
        Preconditions.checkNotNull(publisher);
        
        Role role = Role.fromKey(roleKey);
        return new CrewMember(null, null, publisher)
            .withRole(role)
            .withName(name);
    }
    
    public static CrewMember crewMember(String id, String name, String roleKey, Publisher publisher) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "ID is null or blank");
        Preconditions.checkNotNull(publisher);
        Role role = Role.fromKey(roleKey);
        String uri = String.format(Person.BASE_URI, publisher.key(), id);
        String curie = publisher.key()+':'+id;
        return new CrewMember(uri, curie, publisher)
            .withRole(role)
            .withName(name);
    }
    
    public Person toPerson() {
        return new Person(this.getCanonicalUri(), this.getCurie(), this.publisher()).withName(name).withProfileLinks(profileLinks());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CrewMember) {
            CrewMember crew = (CrewMember) obj;
            return this.getCanonicalUri().equals(crew.getCanonicalUri()) && name.equals(crew.name) && role == crew.role;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return getCanonicalUri().hashCode();
    }
    
    @Override
    public String toString() {
        return getCanonicalUri();//"Crew "+name+" worked as a "+role.title;
    }
    
    public CrewMember copy() {
        CrewMember crew = new CrewMember();
        copyTo(crew, this);
        return crew;
    }
    
    public static void copyTo(CrewMember to, CrewMember from) {
        Identified.copyTo(from, to);
        to.name = from.name;
        to.publisher = from.publisher;
        to.role = from.role;
    }
    
    public final static Function<CrewMember, CrewMember> COPY = new Function<CrewMember, CrewMember>() {
        @Override
        public CrewMember apply(CrewMember input) {
            return input.copy();
        }
    };
}
