package Dynamoth.Core.Availability;

import Dynamoth.Core.Client.RPubClientId;

/**
 *
 * @author Julien Gascon-Samson
 */
public interface FailureListener {
	void failureDetected(RPubClientId clientId);
}
