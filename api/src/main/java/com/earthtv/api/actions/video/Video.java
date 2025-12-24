package com.earthtv.api.actions.video;

import com.earthtv.api.props.IProps;

import java.sql.*;

import com.earthtv.api.actions.Actions;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class Video extends Actions {

    public Video(IProps item){
        super(item);
    }

    public boolean exists(Connection conn){
        String video_id = this.getProps().getID();
        String sqlQuery = "SELECT * FROM video WHERE query_id='" + video_id + "';";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sqlQuery)) {
            return rs.next();
//            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void create(Connection conn){
        try {
            Statement stmnt = conn.createStatement();
            String video_id = String.valueOf(this.getProps().getID());
            PreparedStatement st = conn.prepareStatement("INSERT INTO video (user_id, start_time, end_time, lon, " +
                    "lat, min_lon, max_lon, min_lat, max_lat, status, query_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            ZoneId eastern = ZoneId.of("America/New_York");
            OffsetDateTime start = ZonedDateTime.ofInstant(
                    Instant.ofEpochSecond(1760514471),
                    eastern
            ).toOffsetDateTime();
            OffsetDateTime end = ZonedDateTime.ofInstant(
                    Instant.ofEpochSecond(1766514471),
                    eastern
            ).toOffsetDateTime();

            st.setInt(1, 0);
            st.setObject(2, start, Types.TIMESTAMP_WITH_TIMEZONE);
            st.setObject(3, end, Types.TIMESTAMP_WITH_TIMEZONE);
            st.setDouble(4, 0.0);
            st.setDouble(5, 0.0);
            st.setDouble(6, 0.0);
            st.setDouble(7, 0.0);
            st.setDouble(8, 0.0);
            st.setDouble(9, 0.0);
            st.setInt(10, 0);
            st.setString(11, this.getProps().getID());
            st.executeUpdate();
            st.close();

//            String insertQuery = "INSERT INTO page (video_id, page_guid) VALUES ";
//            insertQuery = insertQuery + "(" + video_id + ", " + "gauoiimn2we" + ")";
//            stmnt.executeUpdate(insertQuery);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
