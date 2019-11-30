package tk.kafka;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor(access=AccessLevel.PUBLIC, force=true)
public class User {
	private int id;
	private String username;
	private String password;
	
	public String toString() {
		return "id: "+id+", username: "+username+", password: "+password;
	}
}
